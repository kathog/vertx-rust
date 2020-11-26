use crate::vertx::{RUNTIME, ClusterManager, EventBus};
use std::sync::{Arc, RwLock};
use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Method, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::http::Error;
use hyper::client::HttpConnector;
use hyper::server::conn::AddrStream;
use hyper::server::Server;
use log::info;
use tokio::runtime::{Runtime, Builder};
use futures::Future;
use std::thread::spawn;
use hashbrown::HashMap;
use std::borrow::Borrow;

pub struct HttpServer<CM:'static + ClusterManager + Send + Sync> {

    pub port: u16,
    event_bus: Option<Arc<EventBus<CM>>>,
    callers: Arc<RwLock<HashMap<(String, Method), RwLock<Box<dyn FnMut(Request<Body>, Arc<EventBus<CM>>, ) -> Result<Response<Body>, Error> + 'static + Send + Sync>>>>>,
    rt: Runtime,
}

impl <CM:'static + ClusterManager + Send + Sync>HttpServer<CM> {

    pub fn new(event_bus: Option<Arc<EventBus<CM>>>) -> HttpServer<CM> {
        HttpServer {
            port: 0,
            event_bus,
            callers: Arc::new(RwLock::new(HashMap::new())),
            rt: Runtime::new().unwrap(),
        }
    }

    pub fn get<OP>(&mut self, path: &str, mut op: OP)
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {

        self.callers.write().unwrap().insert((path.to_owned(), Method::GET), RwLock::new(Box::new(op)));
    }

    pub fn listen(&mut self, port: u16) {
        let ev = self.event_bus.as_ref().unwrap().clone();

        let mut callers = self.callers.clone();
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let new_service = make_service_fn(move |_conn: &AddrStream| {
            let ev = ev.clone();
            let callers = callers.clone();
            async move {
                let x = Ok::<_, Infallible>(service_fn(move |req : Request<Body>| {
                    let ev = ev.clone();
                    let callers = callers.clone();
                    async move {
                        let ev = ev.to_owned();
                        let callers = callers.to_owned();
                        let callers = callers.read().unwrap();
                        let op = callers.get(&(req.uri().path().to_owned(), req.method().clone()));
                        match op {
                            Some(mut op) => {
                                let mut op = op.write().unwrap();
                                op(req, ev)
                            },
                            None => {
                                Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(b"NOTFOUND".to_vec().into())
                                    .unwrap())
                            }
                        }
                        // op(req, ev)
                    }
                }));

                x
            }
        });

        self.rt.spawn( async move{
            let server = Server::bind(&addr).serve(new_service);
            info!("Listening on http://{}", addr);
            server.await;
        });
    }

}