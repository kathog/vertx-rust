use vertx_rust::vertx::{Vertx, VertxOptions};
use vertx_rust::zk::ZookeeperClusterManager;
use vertx_rust::vertx::message::Body;

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
        let response = format!(
            r#"{{"health": "{code}"}}"#,
            code = body.as_string().unwrap()
        );
        m.reply(Body::String(response));
    });

    vertx.start().await;
}
