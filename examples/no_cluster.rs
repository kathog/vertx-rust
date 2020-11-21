use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};
use vertx_rust::net::NetServer;
use simple_logger::SimpleLogger;
use crossbeam_channel::bounded;

fn main() {
    SimpleLogger::new().init().unwrap();

    let vertx_options = VertxOptions::default();
    let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
    let event_bus = vertx.event_bus();

    event_bus.consumer("test.01", move |m, _| {
        let body = m.body();
        let response = format!("{{\"health\": \"{code}\"}}", code=std::str::from_utf8(&body.to_vec()).unwrap());
        m.reply(response.into_bytes());
    });
    let net_server = NetServer::new(Some(event_bus.clone()));
    net_server.listen(9091, move |_req, ev| {
        let mut resp = vec![];
        let (tx,rx) = bounded(1);
        ev.request_with_callback("test.01", "UP".to_string(), move |m, _| {
            let _ = tx.send(m.body());
        });
        let body = rx.recv().unwrap();
        let data = format!(r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: 16

{json_body}"#, json_body=std::str::from_utf8(&body).unwrap());
        resp.extend_from_slice(data.as_bytes());
        resp
    });
    vertx.start();
}
