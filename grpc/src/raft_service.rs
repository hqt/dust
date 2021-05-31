use tokio::sync::oneshot::{Receiver, Sender};
use tokio::runtime::{Builder, Runtime};
use tonic::transport::Server;
use std::time::Duration;
use raft_api::raft_api_server::{RaftApi, RaftApiServer};
use raft_api::raft_api_client::RaftApiClient;
use raft_api::{HelloReply, HelloRequest};
use tonic::{Request, Response, Status};
use rand::Rng;

// all grpc generated code will be generated inside this module
pub mod raft_api {
    tonic::include_proto!("raft_service"); // The string specified here must match the proto package name
}

// ServiceCore stores all data that need to access across requests
#[derive(Default)]
pub struct ServiceCore {}

#[tonic::async_trait]
impl RaftApi for ServiceCore {
    async fn say_hello(&self, request: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = HelloReply {
            message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply))
    }
}

// RaftService provides a gRPC service to communicate between Raft instances
struct RaftService {
    addr: String,
    thread_pool: Runtime,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
}

impl RaftService {
    pub fn new(thread_size: usize, addr: String) -> RaftService {
        // manually setup runtime environment instead of using the conventional macro #[tokio::main]
        let thread_pool = Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(thread_size)
            .worker_threads(thread_size)
            .thread_name("raft-server")
            .on_thread_start(|| {
                println!("raft server started");
            })
            .on_thread_stop(|| {
                println!("stopping raft server");
            })
            .build().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        RaftService {
            addr,
            thread_pool,
            tx,
            rx: Some(rx),
        }
    }

    pub fn start(&mut self) {
        let addr = self.addr.parse().expect("Unable to parse socket address");

        let rx = self.rx.take().unwrap();
        let core = ServiceCore::default();
        let server = Server::builder()
            .add_service(RaftApiServer::new(core))
            .serve_with_shutdown(addr, async move {
                let _ = rx.await;
            });

        // enter the reactor
        let _incoming = self.thread_pool.enter();

        self.thread_pool.spawn(server);
        eprintln!("started service ...");
    }

    pub fn stop(self) {
        let _ = self.tx.send(());
        self.thread_pool.shutdown_timeout(Duration::from_secs(10));
    }

    pub fn start_test_server(&mut self) {
        let port = rand::thread_rng().gen_range(10_000..65_000);
        self.addr = format!("127.0.0.1:{}", port);
        self.start();
    }

    pub fn listening_addr(&self) -> String {
        self.addr.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::block_on;

    #[test]
    fn test_dump() {
        let mut service = RaftService::new(1, "".to_string());
        service.start_test_server();

        let addr = format!("http://{}", service.listening_addr());
        let handle = service.thread_pool.spawn(async move {
            let mut client = RaftApiClient::connect(addr).await.unwrap();
            let request = tonic::Request::new(HelloRequest {
                name: "Tonic".into(),
            });

            let response = client.say_hello(request).await.unwrap();
            assert_eq!(response.into_inner().message, "Hello Tonic!");
        });

        block_on(handle).unwrap();

        service.stop();
    }
}