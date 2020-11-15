use crate::vertx::{ClusterManager, Vertx};
use std::sync::Arc;
use jvm_serializable::java::io::*;
use serde::{Serialize, Deserialize};
use multimap::MultiMap;
use uuid::Uuid;
use zookeeper::ZooKeeper;
use tokio::time::Duration;
use log::{error, info, LevelFilter, warn};


#[cfg(test)]
mod tests {
    use crate::zk::ZookeeperClusterManager;
    use simple_logger::SimpleLogger;

    #[test]
    fn zk_init () {
        SimpleLogger::new().init().unwrap();
        let zk = ZookeeperClusterManager::new("127.0.0.1:2181".to_string(), "io.vertx.01".to_string());

        info!("{:?}", zk.node_id);
    }

}


static ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH : &str = "/cluster/nodes";
static ZK_PATH_HA_INFO : &str = "/syncMap/__vertx.haInfo";
static ZK_PATH_SUBS : &str = "/asyncMultiMap/__vertx.subs";
pub static ZK_ROOT_NODE : &str = "io.vertx";

#[jvm_object(io.vertx.core.net.impl.ServerID,5636540499169644934)]
struct ServerID {
    port: i32,
    host: String
}


#[jvm_object(io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo,1)]
struct ClusterNodeInfo {
    nodeId: String,
    serverID: ServerID,
}

#[jvm_object(java.lang.Object,0)]
struct Object {
    key: String,
    value: String,
}

#[jvm_object(io.vertx.spi.cluster.zookeeper.impl.ZKSyncMap$KeyValue,6529685098267757690)]
struct ZKSyncMapKeyValue {
    key: Object,
}


struct ZookeeperClusterManager {

    vertx: Option<Arc<Vertx>>,
    node_id: String,
    nodes: Vec<String>,
    ha_infos: Vec<ClusterNodeInfo>,
    subs: MultiMap<String, ClusterNodeInfo>,
    zookeeper: ZooKeeper

}

impl ZookeeperClusterManager {

    pub fn new (zk_hosts: String, zk_root: String) -> ZookeeperClusterManager {
        let zookeeper = ZooKeeper::connect(&format!("{}/{}", zk_hosts, zk_root), Duration::from_secs(15), |x| {}).unwrap();
        ZookeeperClusterManager {
            nodes : Vec::new(),
            vertx: None,
            node_id: Uuid::new_v4().to_string(),
            ha_infos: Vec::new(),
            subs: MultiMap::new(),
            zookeeper
        }
    }

}


impl ClusterManager for ZookeeperClusterManager {

    fn set_vertx(&mut self, vertx: Arc<Vertx>) {
        self.vertx = Some(vertx);
    }

    fn get_node_id(&self) -> String {
        self.node_id.clone()
    }

    fn get_nodes(&self) -> Vec<String> {
        self.nodes.clone()
    }

    fn join(&mut self) {
        unimplemented!()
    }

    fn leave(&self) {
        unimplemented!()
    }
}