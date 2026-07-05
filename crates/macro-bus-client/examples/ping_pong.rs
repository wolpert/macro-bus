//! A tiny two-app demo over the bus, using the client library.
//!
//! One task subscribes to `demo.ping` and, for each ping it receives, publishes
//! a `demo.pong`. The main task registers both types, publishes a few pings,
//! and prints the pongs it gets back — two independent "apps" talking over the
//! bus on one daemon.
//!
//! Run a daemon first, then:
//! ```sh
//! cargo run -p macro-bus-client --example ping_pong -- /tmp/macro-bus.sock
//! ```

use std::time::Duration;

use macro_bus_client::{Client, Event};

const PING: &str = "demo.ping";
const PONG: &str = "demo.pong";
const KEY: &str = "demo-key";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/macro-bus.sock".to_string());

    // "App 1": the responder. Subscribes to pings, replies with pongs.
    let responder_socket = socket.clone();
    let responder = tokio::spawn(async move {
        let mut sub = Client::connect(&responder_socket)
            .await
            .expect("connect responder");
        sub.subscribe(PING).await.expect("subscribe ping");
        // A second connection to publish pongs (one task per connection).
        let mut pubc = Client::connect(&responder_socket)
            .await
            .expect("connect pub");
        loop {
            match sub.next_event().await {
                Ok(Event::Message(m)) => {
                    let who = m.body.first().cloned().unwrap_or_default();
                    println!("[responder] got ping from {who:?}, sending pong");
                    if pubc
                        .publish(PONG, KEY, &[format!("pong for {who}")])
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    // "App 2": the initiator. Owns both types, subscribes to pongs, sends pings.
    let mut app = Client::connect(&socket).await?;
    app.register(PING, KEY).await?;
    app.register(PONG, KEY).await?;
    app.subscribe(PONG).await?;

    // Give the responder a moment to subscribe.
    tokio::time::sleep(Duration::from_millis(200)).await;

    for i in 0..3 {
        println!("[initiator] ping {i}");
        app.publish(PING, KEY, &[format!("ping-{i}")]).await?;
        // Wait for the pong.
        match tokio::time::timeout(Duration::from_secs(2), app.next_event()).await {
            Ok(Ok(Event::Message(m))) => println!("[initiator] <- {}", m.body.join(" ")),
            Ok(Ok(other)) => println!("[initiator] <- (other event: {other:?})"),
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => println!("[initiator] timed out waiting for pong"),
        }
    }

    responder.abort();
    println!("done");
    Ok(())
}
