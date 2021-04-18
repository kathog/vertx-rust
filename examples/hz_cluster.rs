use crossbeam_channel::bounded;
use vertx_rust::vertx::{Vertx, VertxOptions};
use vertx_rust::hz::HazelcastClusterManager;
use vertx_rust::vertx::message::Body;
use hyper::StatusCode;
use hyper::Response;
use vertx_rust::vertx::cm::ClusterManager;

fn main() {
    pretty_env_logger::init_timed();

    // let vertx_options = VertxOptions::default();
    // let mut vertx = Vertx::new(vertx_options);
    let mut hz = HazelcastClusterManager::new();
    hz.join();

}
