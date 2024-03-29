use std::convert::Infallible;
use std::net::SocketAddr;
use std::panic::RefUnwindSafe;
use std::sync::Arc;

use chrono::{DateTime, Local};
use hashbrown::hash_map::{Iter};
use hashbrown::HashMap;
use hyper::{Body, HeaderMap, Method, Response, StatusCode, Uri, Version};
use hyper::body::HttpBody;
use hyper::header::HeaderValue;
use hyper::http::{Error, Extensions};
use hyper::server::conn::AddrStream;
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use log::info;
use regex::{Captures, Regex};
use tokio::runtime::{Builder, Runtime};

use crate::vertx::{cm::ClusterManager, EventBus};

#[cfg(feature = "client")]
pub mod client;


pub struct Request {
    pub request: hyper::Request<Body>,
    pub(crate) paths: HashMap<String, String>,
    pub request_timestamp: DateTime<Local>,
    pub body: Vec<u8>,
}

impl Request {

    #[inline]
    pub fn path_value (&self, key: &str) -> Option<&String> {
        self.paths.get(key)
    }

    #[inline]
    pub fn path_iter (&self) -> Iter<String, String> {
        self.paths.iter()
    }

    #[inline]
    pub fn into_body (self) -> Body {
        self.request.into_body()
    }

    #[inline]
    pub fn method(&self) -> &Method {
        &self.request.method()
    }

    #[inline]
    pub fn uri(&self) -> &Uri {
        &self.request.uri()
    }

    #[inline]
    pub fn version(&self) -> Version {
        self.request.version()
    }

    #[inline]
    pub fn headers(&self) -> &HeaderMap<HeaderValue> {
        &self.request.headers()
    }

    #[inline]
    pub fn extensions(&self) -> &Extensions {
        &self.request.extensions()
    }

    #[inline]
    pub fn body(&self) -> &Body {
        &self.request.body()
    }

}

pub struct HttpServer<CM: 'static + ClusterManager + Send + Sync +  RefUnwindSafe> {
    pub port: u16,
    event_bus: Option<Arc<EventBus<CM>>>,
    callers: Arc<
        HashMap<
            (String, Method),
            Arc<
                dyn FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
                    + 'static
                    + Send
                    + Sync,
            >,
        >,
    >,
    regexes: Arc<HashMap<String, (Regex, String)>>,
    rt: Runtime,
    main_reg: Regex,
}

impl<CM: 'static + ClusterManager + Send + Sync + RefUnwindSafe> HttpServer<CM> {
    pub(crate) fn new(event_bus: Option<Arc<EventBus<CM>>>) -> HttpServer<CM> {
        HttpServer {
            port: 0,
            event_bus,
            callers: Arc::new(HashMap::new()),
            regexes: Arc::new(HashMap::new()),
            main_reg: Regex::new("(:\\w+)").unwrap(),
            rt: Builder::new_multi_thread()
                .worker_threads(12)
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn get<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::GET, op)
    }

    pub fn post<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::POST, op)
    }

    pub fn put<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::PUT, op)
    }

    pub fn delete<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::DELETE, op)
    }

    pub fn head<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::HEAD, op)
    }

    pub fn patch<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::PATCH, op)
    }

    pub fn options<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::OPTIONS, op)
    }

    pub fn connect<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::CONNECT, op)
    }

    pub fn trace<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        self.add_op(path, Method::TRACE, op)
    }

    fn add_op<OP>(&mut self, path: &str, method: Method, op: OP) -> &mut Self
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync,
    {
        let callers = Arc::get_mut(&mut self.callers).unwrap();
        let reg_path = self.main_reg.replace_all(path, "(\\w+)");
        callers.insert((reg_path.to_string(), method), Arc::new(op));

        let regexes = Arc::get_mut(&mut self.regexes).unwrap();
        let path = path.replace(":", "");
        regexes.insert(reg_path.to_string(), (Regex::new(&reg_path).unwrap(), path));

        self
    }

    pub fn listen_with_default<OP>(&mut self, port: u16, mut default: OP)
    where
        OP: FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
            + 'static
            + Send
            + Sync
            + Copy,
    {
        let ev = self.event_bus.as_ref().unwrap().clone();

        let callers = self.callers.clone();
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let new_service = make_service_fn(move |_conn: &AddrStream| {
            let ev = ev.clone();
            let callers = callers.clone();
            async move {
                let x = Ok::<_, Infallible>(service_fn(move |mut req: hyper::Request<Body>| {
                    let ev = ev.clone();
                    let callers = callers.clone();
                    async move {
                        let mut data = vec![];
                        let body = req.body_mut();
                        loop {
                            let bytes = body.data().await;
                            match bytes {
                                Some(bytes) => {
                                    match bytes {
                                        Ok(bytes) => {
                                            data.extend_from_slice(&bytes);
                                        }
                                        Err(_) => {
                                            break;
                                        }
                                    }
                                }
                                None => {break;}
                            }
                        }
                        let ev = ev.to_owned();
                        let op = callers.get(&(req.uri().path().to_owned(), req.method().clone()));
                        let request = Request {
                            request: req,
                            paths: Default::default(),
                            request_timestamp: Local::now(),
                            body: data
                        };
                        match op {
                            Some(op) => {
                                let mut op = op.clone();
                                unsafe {
                                    let op = Arc::get_mut_unchecked(&mut op);
                                    op(request, ev)
                                }
                            }
                            None => default(request, ev),
                        }
                    }
                }));

                x
            }
        });

        self.rt.spawn(async move {
            let server = Server::bind(&addr).serve(new_service);
            info!("start http_server on http://{}", addr);
            let _ = server.await;
        });
    }

    pub fn listen(&mut self, port: u16) {
        let ev = self.event_bus.as_ref().unwrap().clone();

        let callers = self.callers.clone();
        let regexes = self.regexes.clone();
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let new_service = make_service_fn(move |_conn: &AddrStream| {
            let ev = ev.clone();
            let callers = callers.clone();
            let regexes = regexes.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |mut req: hyper::Request<Body>| {
                    let ev = ev.clone();
                    let callers = callers.clone();
                    let regexes = regexes.clone();
                    async move {
                        let mut data = vec![];
                        let body = req.body_mut();
                        loop {
                            let bytes = body.data().await;
                            match bytes {
                                Some(bytes) => {
                                    match bytes {
                                        Ok(bytes) => {
                                            data.extend_from_slice(&bytes);
                                        }
                                        Err(_) => {
                                            break;
                                        }
                                    }
                                }
                                None => {break;}
                            }
                        }
                        let ev = ev.to_owned();
                        let path = req.uri().path().to_owned();
                        let mut path_key = &path;
                        let mut paths = HashMap::new();
                        for (k, v) in regexes.iter() {
                            if v.0.is_match(&path) {
                                path_key = k;
                                let caps_base = v.0.captures_iter(&v.1);
                                let caps : Vec<Captures> = v.0.captures_iter(&path).collect();
                                for (i, c) in caps_base.enumerate() {
                                    for idx in 1..c.len() {
                                        if let Some(name) = c.get(idx) {
                                            if let Some(value) = caps[i].get(idx) {
                                                let _ = paths.insert(name.as_str().to_string(),
                                                                     value.as_str().to_string()
                                                );
                                            }
                                        }
                                    }

                                }
                                break;
                            }
                        }
                        let op = callers.get(&(path_key.to_owned(), req.method().clone()));
                        let request = Request {
                            request: req,
                            paths,
                            request_timestamp: Local::now(),
                            body: data
                        };
                        <HttpServer<CM>>::invoke_function(request, ev, op)
                    }
                }))
            }
        });

        self.rt.spawn(async move {
            let server = Server::bind(&addr).serve(new_service);
            info!("start http_server on http://{}", addr);
            let _ = server.await;
        });
    }

    #[inline]
    fn invoke_function(
        req: Request,
        ev: Arc<EventBus<CM>>,
        op: Option<
            &Arc<
                dyn FnMut(Request, Arc<EventBus<CM>>) -> Result<Response<Body>, Error>
                    + Send
                    + Sync,
            >,
        >,
    ) -> Result<Response<Body>, Error> {
        match op {
            Some(op) => {
                let mut op = op.clone();
                unsafe {
                    let op = Arc::get_mut_unchecked(&mut op);
                    op(req, ev)
                }
            }
            None => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(b"NOTFOUND".to_vec().into())
                .unwrap()),
        }
    }
}
