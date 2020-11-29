use crossbeam_channel::bounded;
use simple_logger::SimpleLogger;
use vertx_rust::vertx::{Vertx, VertxOptions};
use vertx_rust::zk::ZookeeperClusterManager;

fn main() {
    SimpleLogger::new().init().unwrap();

    let vertx_options = VertxOptions::default();
    let mut vertx = Vertx::new(vertx_options);
    let zk = ZookeeperClusterManager::new("127.0.0.1:2181".to_string(), "io.vertx.01".to_string());
    vertx.set_cluster_manager(zk);
    let event_bus = vertx.event_bus();

    event_bus.local_consumer("test.01", move |m, _| {
        let body = m.body();
        let response = format!(
            r#"{{"health": "{code}"}}"#,
            code = std::str::from_utf8(&body.to_vec()).unwrap()
        );
        m.reply(response.into_bytes());
    });
    let net_server = vertx.create_net_server();
    net_server.listen(9091, move |_req, ev| {
        let mut resp = vec![];
        let (tx, rx) = bounded(1);
        ev.request("test.01", b"UP".to_vec(), move |m, _| {
            let _ = tx.send(m.body());
        });
        let body = rx.recv().unwrap();
        let data = format!(
            r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: 16

{json_body}"#,
            json_body = std::str::from_utf8(&body).unwrap()
        );
        resp.extend_from_slice(data.as_bytes());
        resp
    });

    vertx.start();
}
