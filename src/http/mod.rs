use std::sync::Arc;

use tide::{Endpoint};
use tide::http::Method;
use tide::utils::Before;
use tokio::runtime::{Builder, Runtime};

use crate::vertx::{cm::ClusterManager, EventBus};

#[cfg(feature = "client")]
pub mod client;

#[derive(Clone)]
pub struct State<CM: 'static + Clone + ClusterManager + Send + Sync +Clone> {
    pub event_bus: Arc<EventBus<CM>>,
}

pub struct HttpServer<CM: 'static + ClusterManager + Send + Sync +Clone + Clone> {
    pub port: u16,
    rt: Runtime,
    server: tide::Server<State<CM>>
}

impl<CM: 'static + ClusterManager + Send + Sync +Clone + Clone> HttpServer<CM> {
    pub(crate) fn new(event_bus: Arc<EventBus<CM>>) -> HttpServer<CM> {
        let mut app = tide::Server::with_state(State {
            event_bus
        });
        app.with(Before(|mut request: tide::Request<State<CM>>| async move {
            request.set_ext(std::time::Instant::now());
            request
        }));
        HttpServer {
            port: 0,
            rt: Builder::new_multi_thread()
                .worker_threads(12)
                .enable_all()
                .build()
                .unwrap(),
            server: app,
        }
    }

    pub fn get<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Get, op)
    }

    pub fn post<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Post, op)
    }

    pub fn put<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Put, op)
    }

    pub fn delete<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Delete, op)
    }

    pub fn head<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Head, op)
    }

    pub fn patch<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Patch, op)
    }

    pub fn options<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Options, op)
    }

    pub fn connect<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Connect, op)
    }

    pub fn trace<OP>(&mut self, path: &str, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.add_op(path, Method::Trace, op)
    }

    fn add_op<OP>(&mut self, path: &str, method: Method, op: OP) -> &mut Self
    where
        OP: Endpoint<State<CM>>,
    {
        self.server.at(path).method(method, op);

        self
    }

    pub async fn listen(&mut self, port: u16) {
        let server = self.server.clone();
        self.rt.spawn(async move {
            server.listen(format!("0.0.0.0:{}", port)).await.unwrap();
        });
    }
}
