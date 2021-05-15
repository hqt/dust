use hyper::{Server, Request, Body, Response, Method, StatusCode};
use std::net::SocketAddr;
use hyper::service::{make_service_fn, service_fn};
use std::sync::Arc;
use hyper::server::conn::AddrStream;
use hyper::Client;
use futures::{TryFutureExt};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::{Sender, Receiver};
use std::time::Duration;

// Service provides a HTTP service
pub struct Service {
    addr: String,
    socket_addr: Option<SocketAddr>,
    thread_pool: Runtime,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    core: Arc<ServiceCore>,
}

// ServiceCore stores all data that need to access across requests
struct ServiceCore {}

impl Service {
    pub fn new(thread_size: usize, addr: String) -> Service {
        // manually setup runtime environment instead of using the conventional macro #[tokio::main]
        let thread_pool = Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(thread_size)
            .worker_threads(thread_size)
            .thread_name("http-server")
            .on_thread_start(|| {
                println!("http server started");
            })
            .on_thread_stop(|| {
                println!("stopping http server");
            })
            .build().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let core = Arc::new(ServiceCore {});

        Service {
            addr,
            socket_addr: None,
            thread_pool,
            tx,
            rx: Some(rx),
            core,
        }
    }

    pub fn start(&mut self) {
        let addr: SocketAddr = self.addr.parse().expect("Unable to parse socket address");

        // https://www.fpcomplete.com/blog/captures-closures-async/
        // inside the closure, value is captured with the static lifetime.
        // We need to clone everytime coming to the closure.
        // Otherwise, Rust will move the object to avoid outlive the original one, which possibly result in compile error
        let core = self.core.clone();
        // closure for every connection from client
        let service = make_service_fn(move |_conn: &AddrStream| {
            let core = core.clone();
            async move {
                // Create a status service.
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| { // closure for every request
                    let core = core.clone();
                    router(core, req)
                }))
            }
        });

        // enter the reactor
        let _incoming = self.thread_pool.enter();

        let server = Server::bind(&addr).serve(service);
        self.socket_addr = Some(server.local_addr());
        let rx = self.rx.take().unwrap();
        let graceful = server
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .map_err(|e| eprintln!("status server error {:?}", e));
        self.thread_pool.spawn(graceful);
        eprintln!("started service ...");
    }

    pub fn stop(self) {
        let _ = self.tx.send(());
        self.thread_pool.shutdown_timeout(Duration::from_secs(10));
    }

    // Return listening address, this may only be used for outer test to get the real address
    // because we may use "127.0.0.1:0" in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.socket_addr.unwrap()
    }
}

async fn router(_srv: Arc<ServiceCore>, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/ping") => Ok(Response::new(Body::from("pong"))),

        // Return the 404 Not Found for other routes.
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::Uri;
    use tokio_test::block_on;

    #[test]
    fn test_ping() {
        let mut service = Service::new(1, "127.0.0.1:0".to_string());
        service.start();

        let endpoint = Uri::builder()
            .scheme("http")
            .authority(service.listening_addr().to_string().as_str())
            .path_and_query("/ping")
            .build()
            .unwrap();

        let client = Client::new();
        let handle = service.thread_pool.spawn(async move {
            let resp = client.get(endpoint).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);

            let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
            let text = String::from_utf8(bytes.into_iter().collect()).unwrap();
            assert_eq!(text, "pong");
        });

        block_on(handle).unwrap();
        service.stop();
    }
}
