#![feature(async_closure)]

use std::sync::Arc;
use crossbeam_channel::bounded;
use futures::future::{BoxFuture};
use hyper::Response;
use hyper::StatusCode;
use vertx_rust::http::client::WebClient;
use vertx_rust::vertx::message::{Body, Message};
use vertx_rust::vertx::{cm::NoClusterManager, EventBus, Vertx, VertxOptions};

fn invoke_test1 (m: &mut Message, _: Arc<EventBus<NoClusterManager>>) -> BoxFuture<()>{
    let b = m.body();
    let response = format!(r#"{{"health": "{code}"}}"#, code = b.as_i32().unwrap());

    Box::pin(async {
        // async body
        m.reply(Body::ByteArray(response.into_bytes()));
    })
}


#[tokio::main(flavor = "multi_thread")]
async fn main() {
    pretty_env_logger::init_timed();
    console_subscriber::init();

    let mut vertx_options = VertxOptions::default();
    vertx_options
        .event_bus_options()
        .event_bus_pool_size(32)
        .event_bus_queue_size(1024);
    let vertx: Vertx<NoClusterManager> = Vertx::new(vertx_options);
    let event_bus = vertx.event_bus().await;

    event_bus.local_consumer("test.01", invoke_test1);
    let net_server = vertx.create_net_server().await;
    net_server
        .listen(9091, move |_req, ev| {
            let mut resp = vec![];
            let (tx, rx) = bounded(1);
            ev.request("test.01", Body::Int(101), move |m, _| {
                let _ = tx.send(m.body());
            });

            let body = rx.recv().unwrap();
            let body = body.as_bytes().unwrap();
            let data = format!(
                r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: {len}

{json_body}"#,
                len = body.len(),
                json_body = String::from_utf8_lossy(body)
            );
            resp.extend_from_slice(data.as_bytes());
            resp
        })
        .await;

    let mut http_server = vertx.create_http_server().await;
    http_server
        .get("/api/v1/:name/:value", move |req, _| {
            println!("{:?}", req.paths);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(b"{\"health\": \"104\"}".to_vec().into())
                .unwrap())
        })
        .get("/", move |_req, ev| {
            let (tx, rx) = bounded(1);
            ev.request("test.01", Body::Int(102), move |m, _| {
                let _ = tx.send(m.body());
            });
            let body = rx.recv().unwrap();
            let body = body.as_bytes().unwrap();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.clone().into())
                .unwrap())
        })

        .post("/", move |req, _| {
            let body = req.request.into_body();
            let body = WebClient::blocking_body(body).unwrap();

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.into())
                .unwrap())
        })
        .listen(9092);

    vertx.start().await;
}
