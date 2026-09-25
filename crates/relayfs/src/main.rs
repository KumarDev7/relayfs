//! relayfs — one binary, multiple modes.
//!
//!   relayfs --mode server --listen 0.0.0.0:8787                      (public relay hub)
//!   relayfs --mode server --listen 0.0.0.0:8787 --http-listen 8788   (relay hub + remote HTTP MCP)
//!   relayfs --mode target --base-url ws://host:8787 ...              (remote agent)
//!   relayfs --mode mcp    --base-url ws://host:8787 ...              (local MCP server over stdio)
//!   relayfs --mode http   --listen 0.0.0.0:8788 --base-url ws:...    (remote HTTP MCP server with OAuth)
//!   relayfs skill                                                    (print the skill doc)

use clap::{CommandFactory, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "relayfs",
    about = "Remote shell + filesystem over a relay, with true FUSE mount and remote HTTP MCP",
    long_about = "One binary, four modes:\n\n  server  public WebSocket hub (run on a VPS, optionally host HTTP MCP)\n  target  remote agent (run on the machine you want access to)\n  mcp     MCP server (stdio or HTTP transport)\n  http    remote HTTP MCP server with OAuth support (no client binary needed)\n\nBoth target and mcp connect OUT to the server, so no open ports are needed\non either machine. They are paired by a shared token.",
    version,
    after_help = "EXAMPLES:\n  relayfs --mode server --listen 0.0.0.0:8787 --token secret\n  relayfs --mode server --listen 0.0.0.0:8787 --token secret --http-listen 0.0.0.0:8788\n  relayfs --mode target --base-url ws://relay.example.com:8787 --token secret\n  relayfs --mode mcp --base-url ws://relay.example.com:8787 --token secret\n  relayfs --mode http --listen 0.0.0.0:8788 --base-url ws://relay.example.com:8787 --token secret\n  relayfs skill\n\nMCP CLIENT CONFIG (for stdio --mode mcp):\n  {\n    \"mcpServers\": {\n      \"relayfs\": {\n        \"command\": \"/path/to/relayfs\",\n        \"args\": [\"--mode\", \"mcp\", \"--base-url\", \"ws://relay.example.com:8787\"],\n        \"env\": { \"RELAYFS_TOKEN\": \"secret\" }\n      }\n    }\n  }\n\nREMOTE HTTP MCP CLIENT CONFIG (no binary needed on client!):\n  URL: http://your-server:8788/mcp\n  Authorization: Bearer <secret>\n  (or login via browser at http://your-server:8788/login)"
)]
struct Cli {
    /// Which mode to run: server, target, mcp, or http.
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
    /// Local MCP server: exposes the target as MCP tools (stdio or HTTP transport).
    Mcp,
    /// Remote HTTP MCP server with OAuth support: connect directly without client binary.
    Http,
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
                    "http" => Some(Mode::Http),
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
                "http" => Some(Mode::Http),
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

            if let Some(http_addr) = &args.http_listen {
                if http_addr != &args.listen {
                    let relay_token = args.token.clone().unwrap_or_default();
                    let port = args
                        .listen
                        .rsplit_once(':')
                        .map(|(_, p)| p)
                        .unwrap_or("8787");
                    let base_url = format!("ws://127.0.0.1:{port}");
                    let http_addr = http_addr.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                        if let Err(e) = relayfs_bridge::run_http(
                            &http_addr,
                            &base_url,
                            &relay_token,
                            "server-http-mcp",
                            "server-http",
                        )
                        .await
                        {
                            tracing::error!("HTTP MCP server error: {e}");
                        }
                    });

                    return relayfs_relay::run(&args.listen, args.token.as_deref()).await;
                }
            }

            run_unified_server(&args.listen, args.token).await
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
            // Logs to stderr: MCP protocol is spoken on stdout in stdio mode.
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            if let Some(listen) = args.listen.as_deref() {
                relayfs_bridge::run_http(listen, &args.base_url, &args.token, &args.id, &args.name)
                    .await
            } else {
                relayfs_bridge::run(&args.base_url, &args.token, &args.id, &args.name).await
            }
        }
        Some(Mode::Http) => {
            let args = HttpArgs::parse_from(&rest);
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            relayfs_bridge::run_http(
                &args.listen,
                &args.base_url,
                &args.token,
                &args.id,
                &args.name,
            )
            .await
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
    /// Also start the remote HTTP MCP server on this address (e.g. 0.0.0.0:8788).
    #[arg(long, env = "RELAYFS_HTTP_LISTEN")]
    http_listen: Option<String>,
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
    about = "Local or remote MCP server",
    long_about = "MCP server that runs on your machine and exposes a remote machine's\nshell and filesystem as MCP tools. Also hosts the FUSE mount: mount_remote\nmounts a remote directory into your local filesystem, backed by RPC calls\nto the target.\n\nSpeaks MCP over stdio by default, or serves over HTTP when --listen is set."
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
    /// If specified, serve MCP over HTTP with OAuth on this address (e.g. 0.0.0.0:8788) instead of stdio.
    #[arg(long, env = "RELAYFS_LISTEN")]
    listen: Option<String>,
}

#[derive(Parser)]
#[command(
    name = "relayfs http",
    about = "Remote HTTP MCP server",
    long_about = "Starts a remote HTTP MCP server that AI clients (Cursor, Claude, web agents) can connect to directly without needing a local relayfs binary.\n\nSupports Streamable HTTP (/mcp), legacy SSE (/sse), and OAuth 2.0 / token login (/oauth/authorize, /login, /oauth/token)."
)]
struct HttpArgs {
    /// Address to listen on, e.g. 0.0.0.0:8788.
    #[arg(long, env = "RELAYFS_LISTEN", default_value = "0.0.0.0:8788")]
    listen: String,
    /// Relay server base URL, e.g. ws://relay.example.com:8787. The /ws endpoint is appended automatically.
    #[arg(long, env = "RELAYFS_RELAY")]
    base_url: String,
    /// Pairing token shared with the relay (env RELAYFS_TOKEN).
    #[arg(long, env = "RELAYFS_TOKEN")]
    token: String,
    /// Stable id for this mcp client.
    #[arg(long, env = "RELAYFS_BRIDGE_ID", default_value = "http-mcp")]
    id: String,
    /// Human-readable name shown in relay logs.
    #[arg(long, env = "RELAYFS_BRIDGE_NAME", default_value = "remote-http")]
    name: String,
}

async fn run_unified_server(listen: &str, token: Option<String>) -> anyhow::Result<()> {
    let token_str = token.clone().unwrap_or_default();
    let relay_state = relayfs_relay::AppState::new(token);

    // 1. Internal relay on ephemeral loopback port for the HTTP MCP bridge
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_port = internal_listener.local_addr()?.port();
    let internal_router = relayfs_relay::make_router(relay_state.clone());

    tokio::spawn(async move {
        if let Err(e) = axum::serve(internal_listener, internal_router).await {
            tracing::error!("internal relay error: {e}");
        }
    });

    // 2. Connect bridge to the internal relay
    let internal_ws_url = format!("ws://127.0.0.1:{internal_port}");
    let client = relayfs_bridge::AgentClient::connect(
        &internal_ws_url,
        &token_str,
        "server-bridge",
        "server-http",
    )
    .await?;

    let server = relayfs_bridge::RelayfsServer::new(std::sync::Arc::new(client));
    let http_state = relayfs_bridge::HttpState::new(token_str);

    // Clean up expired OAuth codes periodically
    let auth_codes = http_state.auth_codes.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let now = std::time::Instant::now();
            auth_codes
                .write()
                .await
                .retain(|_, entry| entry.expires_at > now);
        }
    });

    // 3. Build unified router: HTTP MCP routes + /ws relay endpoint
    let http_app = relayfs_bridge::make_router(http_state, server);
    let ws_app = axum::Router::new()
        .route("/ws", axum::routing::get(relayfs_relay::ws_handler))
        .with_state(relay_state);

    let unified_app = http_app.merge(ws_app);

    // 4. Bind public listener
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("relayfs server listening on http/ws://{listen}");
    tracing::info!("  WebSocket endpoint : ws://{listen}/ws");
    tracing::info!("  HTTP MCP endpoint  : http://{listen}/mcp");
    tracing::info!("  OAuth login UI     : http://{listen}/login");
    tracing::info!("  Health check       : http://{listen}/healthz");

    axum::serve(listener, unified_app).await?;
    Ok(())
}

