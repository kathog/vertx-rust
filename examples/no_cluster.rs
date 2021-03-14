use crossbeam_channel::bounded;
use hyper::Response;
use hyper::StatusCode;
use vertx_rust::http::client::WebClient;
use vertx_rust::vertx::{cm::NoClusterManager, Vertx, VertxOptions};
use vertx_rust::vertx::message::Body;

fn main() {
    pretty_env_logger::init_timed();

    let mut vertx_options = VertxOptions::default();
    vertx_options
        .event_bus_options()
        .event_bus_pool_size(6)
        .event_bus_queue_size(1024);
    let vertx: Vertx<NoClusterManager> = Vertx::new(vertx_options);
    let event_bus = vertx.event_bus();

    event_bus.consumer("test.01", move |m, _| {
        let b = m.body();
        let response = format!(
            r#"{{"health": "{code}"}}"#,
            code = b.as_i32().unwrap()
        );
        m.reply(Body::String(response));
    });
    let net_server = vertx.create_net_server();
    net_server.listen(9091, move |_req, ev| {
        let mut resp = vec![];
        let (tx, rx) = bounded(1);
        ev.request(
            "test.01",
            Body::Int(101),
            move |m, _| {
                let _ = tx.send(m.body());
            },
        );
        let body = rx.recv().unwrap();
        let body = body.as_string().unwrap();
        let data = format!(
            r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: {len}

{json_body}"#,
            len = body.len(),
            json_body = body
        );
        resp.extend_from_slice(data.as_bytes());
        resp
    });

    let mut http_server = vertx.create_http_server();
    http_server
        .get("/", move |_req, ev| {
            let (tx, rx) = bounded(1);
            ev.request("test.01", Body::Int(102), move |m, _| {
                let _ = tx.send(m.body());
            });
            let body = rx.recv().unwrap();
            let body = body.as_string().unwrap();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.clone().into())
                .unwrap())
        })
        .post("/", move |req, _| {
            let body = req.into_body();
            let body = WebClient::blocking_body(body).unwrap();

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.into())
                .unwrap())
        })
        .listen_with_default(9092, move |_, _| {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body("NIMA".as_bytes().into())
                .unwrap())
        });

    vertx.start();
}
