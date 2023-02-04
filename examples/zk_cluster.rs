use crossbeam_channel::bounded;
use vertx_rust::vertx::{Vertx, VertxOptions};
use vertx_rust::zk::ZookeeperClusterManager;
use vertx_rust::vertx::message::Body;
use hyper::StatusCode;
use hyper::Response;

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();

    let vertx_options = VertxOptions::default();
    let mut vertx = Vertx::new(vertx_options);
    let zk = ZookeeperClusterManager::new("127.0.0.1:2181".to_string(), "io.vertx".to_string());
    vertx.set_cluster_manager(zk);
    let event_bus = vertx.event_bus().await;

    event_bus.consumer("test.01", move |m, _| {
        let body = m.body();
        Box::pin(async move {
            let response = format!(
                r#"{{"health": "{code}"}}"#,
                code = body.as_string().unwrap()
            );
            m.reply(Body::String(response));
        })
    });

    let mut http_server = vertx.create_http_server().await;
    http_server
        .get("/", move |_req, ev| {
            let (tx, rx) = bounded(1);
            ev.request("test.01", Body::String("UP".to_string()), move |m, _| {
                let _ = tx.send(m.body());
            });
            let body = rx.recv().unwrap();
            let body = body.as_string().unwrap();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.clone().into())
                .unwrap())
        }).listen_with_default(9091, move |_, _| {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body("NOTFOUND".as_bytes().into())
                .unwrap())
        });
    vertx.start().await;
}
