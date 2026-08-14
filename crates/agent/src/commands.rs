//! Shell command execution with live output streaming.
//!
//! A command runs under the user's login shell. stdout and stderr are
//! forwarded to the bridge as `command_output` notifications while the
//! command runs; the final `run_command` response carries the exit status.
//!
//! With `wait: false` the response returns an `execution_id` immediately and
//! the command keeps running in the background; `get_command_result` fetches
//! the result (running or finished) by that id.

use std::process::Stdio;
use std::sync::Arc;

use relayfs_protocol::{
    CommandFinishedNotification, CommandOutputNotification, GetCommandResult,
    GetCommandResultParams, RunCommandParams, RunCommandResult,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::Mutex;
use tracing::warn;

use crate::conn::{send_notification, send_response, WsSink};
use crate::AgentState;

/// A running command tracked in `AgentState.commands`.
pub struct RunningCommand {
    pub child: Child,
}

/// Result of a command started with `wait: false`, tracked in
/// `AgentState.results` from spawn until it is fetched.
pub struct CommandResult {
    /// True once the command has exited.
    pub done: bool,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    /// Output so far; the background task appends as lines arrive.
    pub output: Arc<Mutex<String>>,
}

/// Run a command, streaming output, and reply when it exits.
pub async fn run_command(
    sink: &WsSink,
    state: &Arc<AgentState>,
    id: u64,
    params: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let params: RunCommandParams = serde_json::from_value(params)?;
    if params.command.trim().is_empty() {
        return Err(anyhow::anyhow!("command must not be empty"));
    }

    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".into());
    let mut cmd = Command::new(&shell);
    cmd.arg("-c")
        .arg(&params.command)
        // stdin is piped so the bridge can feed input; stdout/stderr are
        // piped and forwarded as notifications — nothing is printed to the
        // target's terminal.
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(cwd) = &params.cwd {
        cmd.current_dir(cwd);
    }
    if let Some(env) = &params.env {
        for (k, v) in env {
            if let Some(v) = v.as_str() {
                cmd.env(k, v);
            }
        }
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn `{}`: {e}", params.command))?;

    // Feed the provided input, then close stdin so the child sees EOF.
    if let Some(input) = &params.input {
        let mut stdin = child.stdin.take().expect("stdin piped");
        if let Err(e) = stdin.write_all(input.as_bytes()).await {
            warn!("failed to write command input: {e}");
        }
        drop(stdin);
    }

    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");

    // Track the child so it can be killed on disconnect.
    state
        .commands
        .lock()
        .await
        .insert(id, RunningCommand { child });

    // Fire-and-forget mode: respond with the execution id now, stream output
    // in the background, and store the result for `get_command_result`.
    if !params.wait.unwrap_or(true) {
        let sink = sink.clone();
        let state = state.clone();
        let output = Arc::new(Mutex::new(String::new()));
        state.results.lock().await.insert(
            id,
            CommandResult {
                done: false,
                exit_code: None,
                timed_out: false,
                output: output.clone(),
            },
        );
        tokio::spawn(async move {
            let outcome = run_to_completion(
                &sink,
                &state,
                id,
                stdout,
                stderr,
                &output,
                params.timeout_secs,
            )
            .await;
            let mut results = state.results.lock().await;
            if let Some(r) = results.get_mut(&id) {
                match outcome {
                    Ok((exit_code, timed_out)) => {
                        r.exit_code = exit_code;
                        r.timed_out = timed_out;
                    }
                    Err(e) => warn!("background command {id} failed: {e}"),
                }
                r.done = true;
            }
        });
        return Ok(serde_json::to_value(RunCommandResult {
            exit_code: None,
            timed_out: false,
            output: String::new(),
            execution_id: Some(id),
        })?);
    }

    // Wait mode: stream into a local buffer, then respond with the full
    // result.
    let output = Mutex::new(String::new());
    let (exit_code, timed_out) = run_to_completion(
        sink,
        state,
        id,
        stdout,
        stderr,
        &output,
        params.timeout_secs,
    )
    .await?;
    let output = output.into_inner();

    state.commands.lock().await.remove(&id);

    // Transparency: report completion (and how it ended) on the target side.
    match (exit_code, timed_out) {
        (_, true) => tracing::info!("command finished: timed out"),
        (Some(0), false) => tracing::info!("command finished: exit code 0"),
        (Some(code), false) => tracing::info!("command finished: exit code {code}"),
        (None, false) => tracing::info!("command finished: killed by signal"),
    }

    // Final notification, then the response.
    send_notification(
        sink,
        relayfs_protocol::notify::COMMAND_FINISHED,
        serde_json::to_value(CommandFinishedNotification {
            id,
            exit_code,
            timed_out,
        })?,
    )
    .await?;

    let result = RunCommandResult {
        exit_code,
        timed_out,
        output,
        execution_id: None,
    };
    send_response(sink, id, Some(serde_json::to_value(result)?), None).await?;
    Ok(serde_json::Value::Null)
}

/// Fetch the result of a command started with `wait: false`.
pub async fn get_command_result(
    state: &AgentState,
    params: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let params: GetCommandResultParams = serde_json::from_value(params)?;
    let results = state.results.lock().await;
    let Some(r) = results.get(&params.execution_id) else {
        return Err(anyhow::anyhow!(
            "unknown execution id: {}",
            params.execution_id
        ));
    };
    let output = r.output.lock().await.clone();
    let output = trim_output(&output, params.head, params.tail);
    Ok(serde_json::to_value(GetCommandResult {
        done: r.done,
        exit_code: if r.done { r.exit_code } else { None },
        timed_out: if r.done { Some(r.timed_out) } else { None },
        output,
    })?)
}

/// Stream stdout/stderr into `output` (forwarding each line as a
/// notification), wait for exit, and return `(exit_code, timed_out)`.
/// The whole thing runs under the timeout: a silent long-running command
/// (no output) must still be killed on time.
async fn run_to_completion(
    sink: &WsSink,
    state: &AgentState,
    id: u64,
    stdout: ChildStdout,
    stderr: ChildStderr,
    output: &Mutex<String>,
    timeout_secs: Option<u64>,
) -> anyhow::Result<(Option<i32>, bool)> {
    let run = async {
        stream_output(sink, id, stdout, stderr, output).await?;
        // Wait for exit.
        let mut guard = state.commands.lock().await;
        let child = &mut guard.get_mut(&id).expect("command tracked").child;
        let status = child.wait().await?;
        Ok::<_, anyhow::Error>(status.code())
    };

    let (exit_code, timed_out) = match timeout_secs {
        Some(secs) if secs > 0 => {
            match tokio::time::timeout(std::time::Duration::from_secs(secs), run).await {
                Ok(Ok(code)) => (code, false),
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    // Deadline hit: kill the child and report the timeout.
                    let mut guard = state.commands.lock().await;
                    let child = &mut guard.get_mut(&id).expect("command tracked").child;
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    (None, true)
                }
            }
        }
        _ => match run.await {
            Ok(code) => (code, false),
            Err(e) => return Err(e),
        },
    };
    Ok((exit_code, timed_out))
}

/// Forward stdout/stderr lines as `command_output` notifications and append
/// them to `output`. A dead connection does not abort the command — the
/// result is still recorded.
async fn stream_output(
    sink: &WsSink,
    id: u64,
    stdout: ChildStdout,
    stderr: ChildStderr,
    output: &Mutex<String>,
) -> anyhow::Result<()> {
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let (mut stdout_done, mut stderr_done) = (false, false);
    while !(stdout_done && stderr_done) {
        tokio::select! {
            line = stdout_lines.next_line(), if !stdout_done => {
                match line {
                    Ok(Some(line)) => {
                        {
                            let mut o = output.lock().await;
                            o.push_str(&line);
                            o.push('\n');
                        }
                        let _ = send_notification(
                            sink,
                            relayfs_protocol::notify::COMMAND_OUTPUT,
                            serde_json::to_value(CommandOutputNotification {
                                id,
                                stream: "stdout".into(),
                                data: line + "\n",
                            })?,
                        )
                        .await;
                    }
                    Ok(None) => stdout_done = true,
                    Err(e) => {
                        warn!("stdout read error: {e}");
                        stdout_done = true;
                    }
                }
            }
            line = stderr_lines.next_line(), if !stderr_done => {
                match line {
                    Ok(Some(line)) => {
                        {
                            let mut o = output.lock().await;
                            o.push_str(&line);
                            o.push('\n');
                        }
                        let _ = send_notification(
                            sink,
                            relayfs_protocol::notify::COMMAND_OUTPUT,
                            serde_json::to_value(CommandOutputNotification {
                                id,
                                stream: "stderr".into(),
                                data: line + "\n",
                            })?,
                        )
                        .await;
                    }
                    Ok(None) => stderr_done = true,
                    Err(e) => {
                        warn!("stderr read error: {e}");
                        stderr_done = true;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Apply head (first N lines) then tail (last N lines) to the output.
fn trim_output(output: &str, head: Option<usize>, tail: Option<usize>) -> String {
    let mut lines: Vec<&str> = output.lines().collect();
    if let Some(h) = head {
        lines.truncate(h);
    }
    if let Some(t) = tail {
        let start = lines.len().saturating_sub(t);
        lines.drain(..start);
    }
    lines.join("\n")
}

/// Kill all running commands (called on disconnect).
pub async fn kill_all(state: &AgentState) {
    let mut commands = state.commands.lock().await;
    for running in commands.values_mut() {
        let _ = running.child.kill().await;
    }
    commands.clear();
}
