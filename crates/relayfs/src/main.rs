//! relayfs — one binary, three modes.
//!
//!   relayfs --mode server --listen 0.0.0.0:8787          (public relay hub)
//!   relayfs --mode target --base-url ws://host:8787 ...  (remote agent)
//!   relayfs --mode mcp    --base-url ws://host:8787 ...  (local MCP server)
//!   relayfs skill                                        (print the skill doc)

use clap::{CommandFactory, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "relayfs",
    about = "Remote shell + filesystem over a relay, with a true FUSE mount",
    long_about = "One binary, three modes:\n\n  server  public WebSocket hub (run on a VPS)\n  target  remote agent (run on the machine you want access to)\n  mcp     local MCP server (run on your machine; exposes the target as MCP tools)\n\nBoth target and mcp connect OUT to the server, so no open ports are needed\non either machine. They are paired by a shared token.",
    version,
    after_help = "EXAMPLES:\n  relayfs --mode server --listen 0.0.0.0:8787 --token secret\n  relayfs --mode target --base-url ws://relay.example.com:8787 --token secret\n  relayfs --mode mcp --base-url ws://relay.example.com:8787 --token secret\n  relayfs skill\n\nMCP CLIENT CONFIG (for --mode mcp):\n  {\n    \"mcpServers\": {\n      \"relayfs\": {\n        \"command\": \"/path/to/relayfs\",\n        \"args\": [\"--mode\", \"mcp\", \"--base-url\", \"ws://relay.example.com:8787\"],\n        \"env\": { \"RELAYFS_TOKEN\": \"secret\" }\n      }\n    }\n  }"
)]
struct Cli {
    /// Which mode to run: server, target, or mcp.
    #[arg(long, value_enum)]
    mode: Option<Mode>,
    /// Optional subcommand.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Public WebSocket hub that pairs targets and mcp clients.
    Server,
    /// Remote agent: runs on the machine you want shell + filesystem access to.
    Target,
    /// Local MCP server: exposes the target as MCP tools (stdio transport).
    Mcp,
}

#[derive(Subcommand)]
enum Command {
    /// Print the relayfs skill document (app overview, principles, caveats).
    Skill,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Extract `--mode <value>` (or `--mode=<value>`) manually so the remaining
    // args can be parsed by the mode's own argument parser.
    let argv: Vec<String> = std::env::args().collect();
    let mut mode: Option<Mode> = None;
    let mut rest: Vec<String> = vec![argv[0].clone()];
    let mut i = 1;
    while i < argv.len() {
        if argv[i] == "--mode" {
            if let Some(v) = argv.get(i + 1) {
                mode = match v.as_str() {
                    "server" => Some(Mode::Server),
                    "target" => Some(Mode::Target),
                    "mcp" => Some(Mode::Mcp),
                    _ => None,
                };
                i += 2;
                continue;
            }
        } else if let Some(v) = argv[i].strip_prefix("--mode=") {
            mode = match v {
                "server" => Some(Mode::Server),
                "target" => Some(Mode::Target),
                "mcp" => Some(Mode::Mcp),
                _ => None,
            };
            i += 1;
            continue;
        }
        rest.push(argv[i].clone());
        i += 1;
    }

    match mode {
        Some(Mode::Server) => {
            let args = ServerArgs::parse_from(&rest);
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            relayfs_relay::run(&args.listen, args.token.as_deref()).await
        }
        Some(Mode::Target) => {
            let args = TargetArgs::parse_from(&rest);
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            relayfs_agent::run(
                &args.base_url,
                &args.token,
                &args.id,
                &args.name,
                args.reconnect_secs,
            )
            .await
        }
        Some(Mode::Mcp) => {
            let args = McpArgs::parse_from(&rest);
            // Logs to stderr: MCP protocol is spoken on stdout.
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            relayfs_bridge::run(&args.base_url, &args.token, &args.id, &args.name).await
        }
        None => {
            // No mode: only help, version, or the skill subcommand.
            let cli = Cli::parse();
            if let Some(Command::Skill) = cli.command {
                relayfs_skill::print_skill();
                return Ok(());
            }
            // No mode and no subcommand: show usage and exit.
            let mut cmd = Cli::command();
            let _ = cmd.print_help();
            println!();
            std::process::exit(2);
        }
    }
}

#[derive(Parser)]
#[command(
    name = "relayfs server",
    about = "Public WebSocket hub",
    long_about = "Public WebSocket hub that pairs relayfs targets and mcp clients.\n\nBoth the mcp client (your machine) and the target (remote machine) connect\nOUT to this server, so neither needs a public IP or open ports. Peers are\npaired by a shared token; JSON-RPC frames are forwarded between them without\ninspection.",
    after_help = "ENDPOINTS:\n  /ws       WebSocket endpoint for targets and mcp clients\n  /healthz  health check (returns 'ok')"
)]
struct ServerArgs {
    /// Address to listen on, e.g. 0.0.0.0:8787.
    #[arg(long, default_value = "0.0.0.0:8787")]
    listen: String,
    /// Required pairing token (env RELAYFS_TOKEN). If unset, any token is accepted.
    #[arg(long, env = "RELAYFS_TOKEN")]
    token: Option<String>,
}

#[derive(Parser)]
#[command(
    name = "relayfs target",
    about = "Remote agent",
    long_about = "Daemon that runs on the remote machine and serves shell execution and\nfile access to a paired relayfs mcp client.\n\nConnects OUT to the relay server (no open ports needed on this machine),\nauthenticates with the pairing token, and reconnects automatically if the\nconnection drops."
)]
struct TargetArgs {
    /// Relay server base URL, e.g. ws://relay.example.com:8787. The /ws endpoint is appended automatically.
    #[arg(long, env = "RELAYFS_RELAY")]
    base_url: String,
    /// Pairing token shared with the relay (env RELAYFS_TOKEN).
    #[arg(long, env = "RELAYFS_TOKEN")]
    token: String,
    /// Stable id for this target (env RELAYFS_AGENT_ID).
    #[arg(long, env = "RELAYFS_AGENT_ID", default_value = "agent")]
    id: String,
    /// Human-readable name shown in relay logs (env RELAYFS_AGENT_NAME).
    #[arg(long, env = "RELAYFS_AGENT_NAME", default_value = "remote")]
    name: String,
    /// Reconnect delay in seconds after a dropped connection.
    #[arg(long, default_value = "5")]
    reconnect_secs: u64,
}

#[derive(Parser)]
#[command(
    name = "relayfs mcp",
    about = "Local MCP server",
    long_about = "MCP server that runs on your machine and exposes a remote machine's\nshell and filesystem as MCP tools. Also hosts the FUSE mount: mount_remote\nmounts a remote directory into your local filesystem, backed by RPC calls\nto the target.\n\nSpeaks MCP over stdio. Configure it as an MCP server in your client\n(Claude Desktop, Cursor, etc.)."
)]
struct McpArgs {
    /// Relay server base URL, e.g. ws://relay.example.com:8787. The /ws endpoint is appended automatically.
    #[arg(long, env = "RELAYFS_RELAY")]
    base_url: String,
    /// Pairing token shared with the relay (env RELAYFS_TOKEN).
    #[arg(long, env = "RELAYFS_TOKEN")]
    token: String,
    /// Stable id for this mcp client.
    #[arg(long, env = "RELAYFS_BRIDGE_ID", default_value = "bridge")]
    id: String,
    /// Human-readable name shown in relay logs.
    #[arg(long, env = "RELAYFS_BRIDGE_NAME", default_value = "local")]
    name: String,
}
