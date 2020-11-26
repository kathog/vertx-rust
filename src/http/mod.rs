use crate::vertx::{ClusterManager, EventBus};
use std::sync::{Arc};
use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Method, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::http::Error;
use hyper::server::conn::AddrStream;
use hyper::server::Server;
use log::info;
use tokio::runtime::{Runtime};
use hashbrown::HashMap;

pub struct HttpServer<CM:'static + ClusterManager + Send + Sync> {

    pub port: u16,
    event_bus: Option<Arc<EventBus<CM>>>,
    callers: Arc<HashMap<(String, Method), Arc<dyn FnMut(Request<Body>, Arc<EventBus<CM>>, ) -> Result<Response<Body>, Error> + 'static + Send + Sync>>>,
    rt: Runtime,
}

impl <CM:'static + ClusterManager + Send + Sync>HttpServer<CM> {

    pub fn new(event_bus: Option<Arc<EventBus<CM>>>) -> HttpServer<CM> {
        HttpServer {
            port: 0,
            event_bus,
            callers: Arc::new(HashMap::new()),
            rt: Runtime::new().unwrap(),
        }
    }

    pub fn get<OP>(&mut self, path: &str, op: OP)
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        let callers = Arc::get_mut(&mut self.callers).unwrap();
        callers.insert((path.to_owned(), Method::GET), Arc::new(op));
    }

    pub fn listen(&mut self, port: u16) {
        let ev = self.event_bus.as_ref().unwrap().clone();

        let callers = self.callers.clone();
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
                        let op = callers.get(&(req.uri().path().to_owned(), req.method().clone()));
                        match op {
                            Some(op) => {
                                let mut op = op.clone();
                                unsafe {
                                    let op = Arc::get_mut_unchecked(&mut op);
                                    op(req, ev)
                                }
                            },
                            None => {
                                Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(b"NOTFOUND".to_vec().into())
                                    .unwrap())
                            }
                        }
                    }
                }));

                x
            }
        });

        self.rt.spawn( async move{
            let server = Server::bind(&addr).serve(new_service);
            info!("Listening on http://{}", addr);
            let _ = server.await;
        });
    }

}