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
use tokio::runtime::{Runtime, Builder};
use hashbrown::HashMap;

pub struct HttpServer<CM:'static + ClusterManager + Send + Sync> {

    pub port: u16,
    event_bus: Option<Arc<EventBus<CM>>>,
    callers: Arc<HashMap<(String, Method), Arc<dyn FnMut(Request<Body>, Arc<EventBus<CM>>, ) -> Result<Response<Body>, Error> + 'static + Send + Sync>>>,
    rt: Runtime,
}

impl <CM:'static + ClusterManager + Send + Sync>HttpServer<CM> {

    pub(crate) fn new(event_bus: Option<Arc<EventBus<CM>>>) -> HttpServer<CM> {
        HttpServer {
            port: 0,
            event_bus,
            callers: Arc::new(HashMap::new()),
            rt: Builder::new_multi_thread().worker_threads(12).enable_all().build().unwrap(),
        }
    }

    pub fn get<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::GET, op)
    }

    pub fn post<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::POST, op)
    }

    pub fn put<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::PUT, op)
    }

    pub fn delete<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::DELETE, op)
    }

    pub fn head<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::HEAD, op)
    }

    pub fn patch<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::PATCH, op)
    }

    pub fn options<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::OPTIONS, op)
    }

    pub fn connect<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::CONNECT, op)
    }

    pub fn trace<OP>(&mut self, path: &str, op: OP) -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {
        self.add_op(path, Method::TRACE, op)
    }

    fn add_op<OP>(&mut self, path: &str, method: Method,  op: OP)  -> &mut Self
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync {

        let callers = Arc::get_mut(&mut self.callers).unwrap();
        callers.insert((path.to_owned(), method), Arc::new(op));

        return self;
    }

    pub fn listen_with_default<OP>(&mut self, port: u16, default: OP)
        where OP: FnMut(Request<Body>,Arc<EventBus<CM>>,) -> Result<Response<Body>, Error> + 'static + Send + Sync + Copy {
        let ev = self.event_bus.as_ref().unwrap().clone();

        let mut default = default;

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
                                default(req, ev)
                            }
                        }
                    }
                }));

                x
            }
        });

        self.rt.spawn( async move{
            let server = Server::bind(&addr).serve(new_service);
            info!("start http_server on http://{}", addr);
            let _ = server.await;
        });
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
                        <HttpServer<CM>>::invoke_function(req, ev, op)
                    }
                }));

                x
            }
        });

        self.rt.spawn( async move{
            let server = Server::bind(&addr).serve(new_service);
            info!("start http_server on http://{}", addr);
            let _ = server.await;
        });
    }

    #[inline]
    fn invoke_function(req: Request<Body>, ev: Arc<EventBus<CM>>, op: Option<&Arc<dyn FnMut(Request<Body>, Arc<EventBus<CM>>) -> Result<Response<Body>, Error> + Send + Sync>>) -> Result<Response<Body>, Error>  {
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
}