#![feature(async_closure)]

use std::sync::Arc;
use std::time::Instant;
use crossbeam_channel::bounded;
use futures::future::{BoxFuture};
use tide::{Request, Response};
use tide::http::mime;
use vertx_rust::http::State;
use vertx_rust::vertx::message::{Body, Message};
use vertx_rust::vertx::{cm::NoClusterManager, EventBus, Vertx, VertxOptions};

fn invoke_test1 (m: Arc<Message>, ev: Arc<EventBus<NoClusterManager>>) -> BoxFuture<'static, ()>{
    let b = m.body();
    let response = format!(r#"{{"health": "{code}"}}"#, code = b.as_i32().unwrap());
    // println!("hello from consumer test.01");
    Box::pin(async move {
        // async body
        ev.request("test.02", Body::Null, move |_, _| {
            let response = response.clone();
            let m = m.clone();
            Box::pin(async move {
                // println!("hello replay test.02");
                m.reply(Body::ByteArray(response.clone().into_bytes()));
            })
        });
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
    event_bus.local_consumer("test.02", move |m, ev| {
        Box::pin(async move {
            // println!("hello from consumer test.02");
            ev.request("test.03", Body::Null, move |_, _| {
                let m = m.clone();
                Box::pin(async move {
                    // println!("hello replay test.03");
                    m.reply(Body::Null);
                })
            });
        })
    });
    event_bus.local_consumer("test.03", move |m, _| {
        Box::pin(async move {
            // println!("hello from consumer test.03");
            m.reply(Body::Null);
        })
    });
    let net_server = vertx.create_net_server().await;
    net_server
        .listen(9091, move |req, ev| {
            println!("{:?}", req.len());
            let mut resp = vec![];
            let (tx, rx) = bounded(1);
            ev.request("test.01", Body::Int(101), move |m, _| {
                let tx = tx.clone();
                Box::pin(async move {let _ = tx.send(m.body());})
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
        .get("/api/v1/:name/:value", async move |req: Request<State<NoClusterManager>>| {
            println!("{:?}", req.param("name"));
            println!("{:?}", req.param("value"));
            Ok(Response::builder(200).content_type(mime::JSON).body(tide::Body::from_string("{\"health\": \"104\"}".to_string())))
        })
        .get("/", async move |req : Request<State<NoClusterManager>>| {
            let (tx, rx) = bounded(1);
            req.state().event_bus.request("test.01", Body::Int(102), move |m, _| {
                let tx = tx.clone();
                Box::pin(async move {
                    // println!("hello from replay test.01");
                    let _ = tx.send(m.body());
                })
            });
            let body = rx.recv().unwrap();
            let body = body.as_bytes().unwrap();

            // println!("{:?}", std::thread::current().id());
            // let body = r#"{"health": "102"}"#.as_bytes();
            // let request_timestamp: &Instant = req.ext().unwrap();
            // println!("{:?}", request_timestamp.elapsed());
            Ok(Response::builder(200).content_type(mime::JSON).body(tide::Body::from_bytes(body.to_vec())))
        })

        .post("/", async move |mut req: Request<State<NoClusterManager>>| {
            let body = req.body_bytes().await.unwrap();
            Ok(Response::builder(200).body(tide::Body::from_bytes(body)))
        })
        // .get("/*", async move |_: Request<State<NoClusterManager>>| {
        //     Ok(Response::builder(200).content_type(mime::PLAIN).body(tide::Body::from_string("NOTFOUND".to_string())))
        // })
        .listen(9092).await;

    vertx.start().await;
}



