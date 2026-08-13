//! Shell command execution with live output streaming.
//!
//! A command runs under the user's login shell. stdout and stderr are
//! forwarded to the bridge as `command_output` notifications while the
//! command runs; the final `run_command` response carries the exit status.

use std::process::Stdio;

use relayfs_protocol::{
    CommandFinishedNotification, CommandOutputNotification, RunCommandParams, RunCommandResult,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tracing::warn;

use crate::conn::{send_notification, send_response, WsStream};
use crate::AgentState;

/// A running command tracked in `AgentState.commands`.
pub struct RunningCommand {
    pub child: Child,
}

/// Run a command, streaming output, and reply when it exits.
pub async fn run_command(
    ws: &mut WsStream,
    state: &AgentState,
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

    let mut output = String::new();
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();

    // Stream both streams concurrently, then wait for exit. The whole thing
    // runs under the timeout: a silent long-running command (no output) must
    // still be killed on time.
    let run = async {
        let (mut stdout_done, mut stderr_done) = (false, false);
        while !(stdout_done && stderr_done) {
            tokio::select! {
                line = stdout_lines.next_line(), if !stdout_done => {
                    match line {
                        Ok(Some(line)) => {
                            output.push_str(&line);
                            output.push('\n');
                            send_notification(
                                ws,
                                relayfs_protocol::notify::COMMAND_OUTPUT,
                                serde_json::to_value(CommandOutputNotification {
                                    id,
                                    stream: "stdout".into(),
                                    data: line + "\n",
                                })?,
                            ).await?;
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
                            output.push_str(&line);
                            output.push('\n');
                            send_notification(
                                ws,
                                relayfs_protocol::notify::COMMAND_OUTPUT,
                                serde_json::to_value(CommandOutputNotification {
                                    id,
                                    stream: "stderr".into(),
                                    data: line + "\n",
                                })?,
                            ).await?;
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
        // Wait for exit.
        let mut guard = state.commands.lock().await;
        let child = &mut guard.get_mut(&id).expect("command tracked").child;
        let status = child.wait().await?;
        Ok::<_, anyhow::Error>(status.code())
    };

    let (exit_code, timed_out) = match params.timeout_secs {
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
        ws,
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
    };
    send_response(ws, id, Some(serde_json::to_value(result)?), None).await?;
    Ok(serde_json::Value::Null)
}

/// Kill all running commands (called on disconnect).
pub async fn kill_all(state: &AgentState) {
    let mut commands = state.commands.lock().await;
    for running in commands.values_mut() {
        let _ = running.child.kill().await;
    }
    commands.clear();
}
