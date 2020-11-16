use crate::vertx::{ClusterManager, Vertx, ClusterNodeInfo};
use std::sync::{Arc, Mutex};
use jvm_serializable::java::io::*;
use serde::{Serialize, Deserialize};
use multimap::MultiMap;
use uuid::Uuid;
use zookeeper::ZooKeeper;
use tokio::time::Duration;
use log::{error, info, LevelFilter, warn};
use zookeeper::recipes::cache::{PathChildrenCache, PathChildrenCacheEvent};
use std::alloc::dealloc;
use std::collections::hash_map::RandomState;


#[cfg(test)]
mod tests {
    use crate::zk::ZookeeperClusterManager;
    use simple_logger::SimpleLogger;
    use tokio::time::Duration;
    use crate::vertx::ClusterManager;

    #[test]
    fn zk_init () {
        SimpleLogger::new().init().unwrap();
        let mut zk = ZookeeperClusterManager::new("127.0.0.1:2181".to_string(), "io.vertx.01".to_string());

        zk.join();
        std::thread::park();
    }

}


static ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH : &str = "/cluster/nodes";
static ZK_PATH_HA_INFO : &str = "/syncMap/__vertx.haInfo";
static ZK_PATH_SUBS : &str = "/asyncMultiMap/__vertx.subs";
pub static ZK_ROOT_NODE : &str = "io.vertx";

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

    node_id: String,
    nodes: Arc<Mutex<Vec<String>>>,
    ha_infos: Arc<Mutex<Vec<ClusterNodeInfo>>>,
    subs: Arc<Mutex<MultiMap<String, ClusterNodeInfo>>>,
    zookeeper: Arc<ZooKeeper>

}

impl ZookeeperClusterManager {

    pub fn new (zk_hosts: String, zk_root: String) -> ZookeeperClusterManager {
        let zookeeper = ZooKeeper::connect(&format!("{}/{}", zk_hosts, zk_root), Duration::from_secs(1), |x| {}).unwrap();
        ZookeeperClusterManager {
            nodes : Arc::new(Mutex::new(Vec::new())),
            node_id: Uuid::new_v4().to_string(),
            ha_infos: Arc::new(Mutex::new(Vec::new())),
            subs: Arc::new(Mutex::new(MultiMap::new())),
            zookeeper: Arc::new(zookeeper)
        }
    }

}


impl ClusterManager for ZookeeperClusterManager {

    fn get_node_id(&self) -> String {
        self.node_id.clone()
    }

    fn get_nodes(&self) -> Vec<String> {
        self.nodes.lock().unwrap().clone()
    }

    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>> {
        self.ha_infos.clone()
    }

    fn get_subs(&self) -> Arc<Mutex<MultiMap<String, ClusterNodeInfo>>> {
        self.subs.clone()
    }

    fn join(&mut self) {
        self.watch_nodes();
        self.watch_ha_info();
        self.watch_subs();
    }

    fn leave(&self) {
        unimplemented!()
    }
}


impl ZookeeperClusterManager {
    fn watch_nodes(&mut self) {
        let mut nodes_cache = PathChildrenCache::new(self.zookeeper.clone(), ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH).unwrap();
        match nodes_cache.start() {
            Err(err) => {
                error!("error starting cache: {:?}", err);
                return;
            }
            _ => {
                debug!("{:?} cache started", ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH);
            }
        }

        let nodes_clone = self.nodes.clone();
        nodes_cache.add_listener(move |event| {
            match event {
                PathChildrenCacheEvent::ChildAdded(_id, data) => {
                    nodes_clone.lock().unwrap().push(String::from_utf8_lossy(&data.0).to_string());
                },
                PathChildrenCacheEvent::ChildRemoved(id) => {
                    let id = id.replace("/cluster/nodes/", "");
                    let mut vec = nodes_clone.lock().unwrap();
                    for (idx, uid) in vec.iter().enumerate() {
                        if id == uid.clone() {
                            vec.remove(idx);
                            break;
                        }
                    }
                },
                _ => {}
            }
        });
    }
}

impl ZookeeperClusterManager {
    fn watch_ha_info(&mut self) {
        let mut ha_info_cache = PathChildrenCache::new(self.zookeeper.clone(), ZK_PATH_HA_INFO).unwrap();
        match ha_info_cache.start() {
            Err(err) => {
                error!("error starting cache: {:?}", err);
                return;
            }
            _ => {
                debug!("{:?} started", ZK_PATH_HA_INFO);
            }
        }
        ha_info_cache.add_listener(move |event| {
            match event {
                PathChildrenCacheEvent::ChildAdded(_id, data) => {
                    let bytes_of_data = data.0.clone();
                    let bytes_as_string = String::from_utf8_lossy(&bytes_of_data);
                    let idx = bytes_as_string.rfind("$");
                    let bytes_as_string = &bytes_as_string[idx.unwrap() + 1..];
                    let mut bytes_as_string_split = bytes_as_string.split("t\u{0}U");
                    let key_value = ZKSyncMapKeyValue {
                        key: Object {
                            key: bytes_as_string_split.next().unwrap().to_string(),
                            value: bytes_as_string_split.next().unwrap().to_string(),
                        }
                    };

                    debug!("{:?}", key_value);
                },
                PathChildrenCacheEvent::ChildRemoved(id) => {}
                _ => {}
            }
        });
    }
}

impl ZookeeperClusterManager {
    fn watch_subs(&mut self) {
        let mut zk_path_subs = PathChildrenCache::new(self.zookeeper.clone(), ZK_PATH_SUBS).unwrap();
        zk_path_subs.start().unwrap();
        let subs: Arc<Mutex<Vec<Mutex<PathChildrenCache>>>> = Arc::new(Mutex::new(Vec::new()));
        let clone_subs = subs.clone();

        let (sender, receiver): (std::sync::mpsc::Sender<String>, std::sync::mpsc::Receiver<String>) = std::sync::mpsc::channel();
        let zk_clone = self.zookeeper.clone();
        let subs_clone = self.subs.clone();
        std::thread::spawn(move || {
            loop {
                match receiver.recv() {
                    Ok(recv) => {
                        let inner_subs = Mutex::new(PathChildrenCache::new(zk_clone.clone(), &recv).unwrap());
                        inner_subs.lock().unwrap().start().unwrap();

                        let inner_subs_clone = subs_clone.clone();

                        inner_subs.lock().unwrap().add_listener(move |ev| {
                            match ev {
                                PathChildrenCacheEvent::ChildAdded(id, data) => {
                                    let msg_addr = recv.replace("/asyncMultiMap/__vertx.subs/", "");
                                    let data_as_byte = &data.0;
                                    let mut deser = ObjectInputStream {};
                                    let node: ClusterNodeInfo = deser.read_object(data_as_byte.clone());
                                    debug!("{:?}", node);
                                    let mut i_subs = inner_subs_clone.lock().unwrap();
                                    i_subs.insert(msg_addr, node);
                                },
                                PathChildrenCacheEvent::ChildRemoved(id) => {
                                    let id = id.replace("/asyncMultiMap/__vertx.subs/", "");
                                    let mut split_id = id.split("/");
                                    let msg_addr = split_id.next().unwrap();
                                    let node_info = split_id.next().unwrap();
                                    let node_info = node_info.split(":").next().unwrap();
                                    let mut i_subs = inner_subs_clone.lock().unwrap();
                                    let subs = i_subs.get_vec_mut(msg_addr).unwrap();
                                    for (idx, node) in subs.iter().enumerate() {
                                        if node.nodeId == node_info {
                                            subs.remove(idx);
                                            break;
                                        }
                                    }
                                },
                                _ => {}
                            }
                        });

                        clone_subs.lock().unwrap().push(inner_subs);
                    },
                    Err(e) => {}
                }
            }
        });

        let sender_clone = sender.clone();

        zk_path_subs.add_listener(move |event| {
            match event {
                PathChildrenCacheEvent::ChildAdded(_id, _data) => {
                    sender_clone.send(_id.clone()).unwrap();
                },
                _ => {}
            }
        });
    }
}
