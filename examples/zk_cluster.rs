use crossbeam_channel::unbounded;
use vertx_rust::vertx::{Vertx, VertxOptions};
use vertx_rust::zk::ZookeeperClusterManager;
use vertx_rust::vertx::message::Body;

fn main() {
    pretty_env_logger::init_timed();

    let vertx_options = VertxOptions::default();
    let mut vertx = Vertx::new(vertx_options);
    let zk = ZookeeperClusterManager::new("127.0.0.1:2181".to_string(), "io.vertx".to_string());
    vertx.set_cluster_manager(zk);
    let event_bus = vertx.event_bus();

    event_bus.consumer("test.01", move |m, _| {
        let body = m.body();
        let body = match body.as_ref() {
            Body::String (s) => s,
            _ => panic!()
        };
        let response = format!(
            r#"{{"health": "{code}"}}"#,
            code = body
        );
        m.reply(Body::String(response));
    });
    let net_server = vertx.create_net_server();
    net_server.listen(9091, move |_req, ev| {
        let mut resp = vec![];
        let (tx, rx) = unbounded();
        ev.request("test.01", Body::String("UP".to_string()), move |m, _| {
            let _ = tx.send(m.body());
        });
        let body = rx.recv().unwrap();
        let body = match body.as_ref() {
            Body::String (s) => s,
            _ => panic!()
        };
        let data = format!(
            r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: 16

{json_body}"#,
            json_body = body
        );
        resp.extend_from_slice(data.as_bytes());
        resp
    });

    vertx.start();
}
