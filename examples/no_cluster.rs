use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};
use simple_logger::SimpleLogger;
use crossbeam_channel::bounded;
use hyper::{StatusCode};
use hyper::Response;
use log::LevelFilter;

fn main() {
    SimpleLogger::new()
        .with_module_level("h2", LevelFilter::Info)
        .with_module_level("hyper", LevelFilter::Info)
        .with_module_level("tracing", LevelFilter::Info).init().unwrap();

    let mut vertx_options = VertxOptions::default();
    vertx_options.event_bus_options().event_bus_pool_size(6).event_bus_queue_size(1024);
    let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
    let event_bus = vertx.event_bus();

    event_bus.consumer("test.01", move |m, _| {
        let body = m.body();
        let response = format!(r#"{{"health": "{code}"}}"#, code=std::str::from_utf8(&body.to_vec()).unwrap());
        m.reply(response.into_bytes());
    });
    let net_server = vertx.create_net_server();
    net_server.listen(9091, move |_req, ev| {
        let mut resp = vec![];
        let (tx,rx) = bounded(1);
        ev.request("test.01", uuid::Uuid::new_v4().to_string().into_bytes(), move |m, _| {
            let _ = tx.send(m.body());
        });
        let body = rx.recv().unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        let data = format!(r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: {len}

{json_body}"#, len=body.len(), json_body=body);
        resp.extend_from_slice(data.as_bytes());
        resp
    });

    let mut http_server = vertx.create_http_server();
    http_server.get("/", move |_req, ev| {
        let (tx,rx) = bounded(1);
        ev.request("test.01", b"UP".to_vec(), move |m, _| {
            let _ = tx.send(m.body());
        });
        let body = rx.recv().unwrap();
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(body.to_vec().into())
            .unwrap())
    }).listen_with_default(9092, move |_, _| {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body("NIMA".as_bytes().into())
            .unwrap())
    });


    vertx.start();
}
