//! `macro-bus` — a thin command-line client and reference tool for MBP/1.0.
//!
//! Examples:
//! ```text
//! macro-bus register sensors.temp s3cr3t
//! macro-bus subscribe sensors.temp                 # prints deliveries
//! echo 21.4C | macro-bus publish sensors.temp s3cr3t
//! macro-bus list
//! ```

use std::io::Read;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use macro_bus_client::{Client, Event};
use macro_bus_proto::DEFAULT_SOCKET_PATH;

/// Command-line client for the macro-bus daemon.
#[derive(Debug, Parser)]
#[command(name = "macro-bus", version, about)]
struct Cli {
    /// Path to the daemon's Unix socket.
    #[arg(short, long, default_value = DEFAULT_SOCKET_PATH, global = true)]
    socket: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Claim ownership of a message type with an auth key (first-registrant wins).
    Register {
        /// The message type to claim.
        type_name: String,
        /// The authorization key to bind to it.
        key: String,
    },
    /// Publish a message. Body is taken from --message, or from stdin if omitted.
    Publish {
        /// The message type to publish to.
        type_name: String,
        /// The authorization key for the type.
        key: String,
        /// The message body (may contain newlines). If omitted, read from stdin.
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Subscribe to one or more types and print deliveries until interrupted.
    Subscribe {
        /// One or more message types to listen for.
        #[arg(required = true)]
        types: Vec<String>,
    },
    /// List known message types.
    List,
    /// Show the daemon's advertised capabilities.
    Capabilities,
    /// Show the daemon's HELP text (the protocol command reference).
    #[command(name = "remote-help")]
    RemoteHelp,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Register { type_name, key } => {
            let mut c = Client::connect(&cli.socket).await?;
            c.register(&type_name, &key).await?;
            println!("registered {type_name}");
            let _ = c.quit().await;
        }
        Command::Publish {
            type_name,
            key,
            message,
        } => {
            let body_text = match message {
                Some(m) => m,
                None => {
                    let mut s = String::new();
                    std::io::stdin().read_to_string(&mut s)?;
                    s
                }
            };
            // Split into lines; drop a single trailing empty line from stdin.
            let mut lines: Vec<&str> = body_text.split('\n').collect();
            if lines.last() == Some(&"") {
                lines.pop();
            }
            let mut c = Client::connect(&cli.socket).await?;
            c.publish(&type_name, &key, &lines).await?;
            println!("published to {type_name} ({} line(s))", lines.len());
            let _ = c.quit().await;
        }
        Command::Subscribe { types } => {
            let mut c = Client::connect(&cli.socket).await?;
            for t in &types {
                c.subscribe(t).await?;
            }
            eprintln!(
                "subscribed to [{}] on {} — waiting for messages (Ctrl-C to stop)",
                types.join(", "),
                c.daemon_id()
            );
            run_subscribe(&mut c).await?;
        }
        Command::List => {
            let mut c = Client::connect(&cli.socket).await?;
            for t in c.list_types().await? {
                println!("{t}");
            }
            let _ = c.quit().await;
        }
        Command::Capabilities => {
            let mut c = Client::connect(&cli.socket).await?;
            for cap in c.capabilities().await? {
                println!("{cap}");
            }
            let _ = c.quit().await;
        }
        Command::RemoteHelp => {
            let mut c = Client::connect(&cli.socket).await?;
            for line in c.help().await? {
                println!("{line}");
            }
            let _ = c.quit().await;
        }
    }
    Ok(())
}

/// Loop printing pushed events until Ctrl-C or the connection closes.
async fn run_subscribe<S>(c: &mut Client<S>) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("interrupted");
                return Ok(());
            }
            ev = c.next_event() => {
                match ev {
                    Ok(Event::Message(m)) => {
                        println!("--- {} [{}] from {}", m.type_name, m.msg_id, m.origin);
                        for line in &m.body {
                            println!("{line}");
                        }
                    }
                    Ok(Event::Drop { type_name, count }) => {
                        eprintln!("[dropped {count} message(s) on {type_name} — consumer fell behind]");
                    }
                    Ok(Event::Note(text)) => {
                        eprintln!("[note] {text}");
                    }
                    Err(macro_bus_client::ClientError::Closed) => {
                        eprintln!("connection closed by daemon");
                        return Ok(());
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
}
