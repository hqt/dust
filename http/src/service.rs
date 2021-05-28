use hyper::{Server, Request, Body, Response, Method, StatusCode};
use std::net::SocketAddr;
use hyper::service::{make_service_fn, service_fn};
use std::sync::{Arc, Mutex};
use hyper::server::conn::AddrStream;
use hyper::Client;
use futures::{TryFutureExt, TryStreamExt};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::{Sender, Receiver};
use std::time::Duration;
use store::{Database, RaftControl};
use serde::Serialize;
use futures::future::ok;
use command::ExecuteRequest;

pub trait DbStore: Database + RaftControl + Clone + Send + 'static {}

// Service provides a HTTP service
pub struct Service<T> where T: DbStore {
    addr: String,
    socket_addr: Option<SocketAddr>,
    thread_pool: Runtime,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    core: ServiceCore<T>,
}

// ServiceCore stores all data that need to access across requests
#[derive(Default, Clone)]
struct ServiceCore<T> where T: DbStore {
    store: Arc<Mutex<T>>,
}

impl<T> Service<T> where T: DbStore {
    pub fn new(thread_size: usize, addr: String, store: T) -> Service<T>
    {
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
        let core = ServiceCore { store: Arc::new(Mutex::new(store)) };

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

async fn router<T>(srv: ServiceCore<T>, req: Request<Body>) -> Result<Response<Body>, hyper::Error> where T: DbStore {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/ping") => Ok(Response::new(Body::from("pong"))),
        (&Method::POST, "/db/execute") => { execute_query(srv.clone(), req).await }

        // Return the 404 Not Found for other routes.
        _ => err_response(StatusCode::NOT_FOUND, "")
    }
}

async fn execute_query<T>(core: ServiceCore<T>, req: Request<Body>) -> hyper::Result<Response<Body>> where T: DbStore {
    let mut body = Vec::new();
    req.into_body()
        .try_for_each(|bytes| {
            body.extend(bytes);
            ok(())
        })
        .await?;

    let r: ExecuteRequest = match serde_json::from_slice(&body) {
        Ok(er) => er,
        Err(err) => {
            return err_response(
                StatusCode::BAD_REQUEST,
                err.to_string(),
            );
        }
    };

    let store = &mut core.store.lock().unwrap();
    return match store.execute(r) {
        Ok(result) => success_response(result),
        Err(err) => err_response(
            StatusCode::BAD_REQUEST,
            err.to_string(),
        )
    };
}

// err_response serialize error request with message and error code
fn err_response<M>(status_code: StatusCode, message: M) -> hyper::Result<Response<Body>>
    where M: Into<Body>
{
    Ok(Response::builder()
        .status(status_code)
        .body(message.into())
        .unwrap()
    )
}

// success_response serializes message to json string
fn success_response<M>(message: M) -> hyper::Result<Response<Body>>
    where M: Serialize
{
    return match serde_json::to_string(&message) {
        Ok(str) => Ok(Response::new(Body::from(str))),
        Err(err) => err_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::Uri;
    use tokio_test::block_on;
    use command::{ExecuteRequest, QueryRequest, Rows, Statement};
    use store::Error;

    #[derive(Default, Clone)]
    struct MockStore {}

    impl RaftControl for MockStore {
        fn join(&mut self, _id: String, _addr: String) -> Result<(), Error> {
            Ok(())
        }

        fn remove(&mut self, _id: String) -> Result<(), Error> {
            Ok(())
        }

        fn leader_id(&self) -> Result<String, Error> {
            Ok("1".to_string())
        }
    }

    impl Database for MockStore {
        fn execute(&mut self, _req: ExecuteRequest) -> Result<Vec<command::Response>, Error> {
            let mut results = Vec::new();
            results.push(command::Response {
                last_insert_id: 1,
                rows_affected: 1,
                error: "".to_string(),
            });
            results.push(command::Response {
                last_insert_id: 2,
                rows_affected: 1,
                error: "".to_string(),
            });
            Ok(results)
        }

        fn query(&self, _req: QueryRequest) -> Result<Rows<'static>, Error> {
            todo!()
        }
    }

    impl DbStore for MockStore {}

    #[test]
    fn test_ping() {
        let mut service = Service::new(1, "127.0.0.1:0".to_string(), MockStore {});
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

    #[test]
    fn test_not_found() {
        let mut service = Service::new(1, "127.0.0.1:0".to_string(), MockStore {});
        service.start();

        let endpoint = Uri::builder()
            .scheme("http")
            .authority(service.listening_addr().to_string().as_str())
            .path_and_query("/random_path")
            .build()
            .unwrap();

        let client = Client::new();
        let handle = service.thread_pool.spawn(async move {
            let resp = client.get(endpoint).await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        });

        block_on(handle).unwrap();
        service.stop();
    }

    #[test]
    fn test_execute_query() {
        let mut service = Service::new(1, "127.0.0.1:0".to_string(), MockStore {});
        service.start();

        let endpoint = Uri::builder()
            .scheme("http")
            .authority(service.listening_addr().to_string().as_str())
            .path_and_query("/db/execute")
            .build()
            .unwrap();

        let mut req = Request::new(Body::from(
            serde_json::to_string(&command::ExecuteRequest {
                request: command::Request {
                    transaction: false,
                    statements: Box::new([
                        Statement {
                            sql: r#"INSERT INTO foo(id, name) VALUES(1, "fiona")"#.to_string(),
                            parameters: Box::new([]),
                        },
                    ]),
                }
            }).unwrap(),
        ));
        *req.method_mut() = Method::POST;
        *req.uri_mut() = endpoint;
        req.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("application/json"),
        );

        let handle = service.thread_pool.spawn(async move {
            let resp = Client::new().request(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);

            let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
            let text = String::from_utf8(bytes.into_iter().collect()).unwrap();
            assert_eq!(
                r#"[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]"#,
                text
            );
        });

        block_on(handle).unwrap();
        service.stop();
    }
}
