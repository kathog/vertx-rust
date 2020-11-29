use crate::vertx::{ClusterManager, ClusterNodeInfo, RUNTIME};
use hashbrown::HashMap;
use jvm_serializable::java::io::*;
use log::info;
use log::{debug, error, warn};
use multimap::MultiMap;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::time::Duration;
use uuid::Uuid;
use zookeeper::recipes::cache::{PathChildrenCache, PathChildrenCacheEvent};
use zookeeper::CreateMode;
use zookeeper::{Acl, ZooKeeper};

static ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH: &str = "/cluster/nodes";
static ZK_PATH_HA_INFO: &str = "/syncMap/__vertx.haInfo";
static ZK_PATH_SUBS: &str = "/asyncMultiMap/__vertx.subs";
pub static ZK_ROOT_NODE: &str = "io.vertx";

#[jvm_object(java.lang.Object,0)]
struct Object {
    key: String,
    value: String,
}

#[jvm_object(io.vertx.spi.cluster.zookeeper.impl.ZKSyncMap$KeyValue,6529685098267757690)]
struct ZKSyncMapKeyValue {
    key: Object,
}

pub struct ZookeeperClusterManager {
    node_id: String,
    nodes: Arc<Mutex<Vec<String>>>,
    ha_infos: Arc<Mutex<Vec<ClusterNodeInfo>>>,
    subs: Arc<Mutex<MultiMap<String, ClusterNodeInfo>>>,
    zookeeper: Arc<ZooKeeper>,
    cluster_node: ClusterNodeInfo,
    tcp_conns: Arc<HashMap<String, Arc<TcpStream>>>,
    cur_idx: Arc<RwLock<usize>>,
}

impl ZookeeperClusterManager {
    #[allow(dead_code)]
    pub fn new(zk_hosts: String, zk_root: String) -> ZookeeperClusterManager {
        let zookeeper =
            ZooKeeper::connect(&format!("{}", zk_hosts), Duration::from_secs(1), |_| {}).unwrap();
        let exist_root = zookeeper.exists(&zk_root, false);
        match exist_root {
            Ok(_) => return { ZookeeperClusterManager::construct(zk_hosts, zk_root) },
            Err(_) => {
                let _ = zookeeper.create(
                    &zk_root,
                    vec![],
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                );
                return ZookeeperClusterManager::construct(zk_hosts, zk_root);
            }
        }
    }

    fn construct(zk_hosts: String, zk_root: String) -> ZookeeperClusterManager {
        let zookeeper = ZooKeeper::connect(
            &format!("{}/{}", zk_hosts, zk_root),
            Duration::from_secs(1),
            |_| {},
        )
        .unwrap();
        ZookeeperClusterManager {
            nodes: Arc::new(Mutex::new(Vec::new())),
            node_id: Uuid::new_v4().to_string(),
            ha_infos: Arc::new(Mutex::new(Vec::new())),
            subs: Arc::new(Mutex::new(MultiMap::new())),
            zookeeper: Arc::new(zookeeper),
            cluster_node: Default::default(),
            tcp_conns: Arc::new(HashMap::new()),
            cur_idx: Arc::new(RwLock::new(0)),
        }
    }
}

impl From<ZooKeeper> for ZookeeperClusterManager {
    fn from(zookeeper: ZooKeeper) -> Self {
        ZookeeperClusterManager {
            nodes: Arc::new(Mutex::new(Vec::new())),
            node_id: Uuid::new_v4().to_string(),
            ha_infos: Arc::new(Mutex::new(Vec::new())),
            subs: Arc::new(Mutex::new(MultiMap::new())),
            zookeeper: Arc::new(zookeeper),
            cluster_node: Default::default(),
            tcp_conns: Arc::new(HashMap::new()),
            cur_idx: Arc::new(RwLock::new(0)),
        }
    }
}

const HEAD_OF_DATA: [u8; 119] = [
    0, 172, 237, 0, 5, 115, 114, 0, 54, 105, 111, 46, 118, 101, 114, 116, 120, 46, 115, 112, 105,
    46, 99, 108, 117, 115, 116, 101, 114, 46, 122, 111, 111, 107, 101, 101, 112, 101, 114, 46, 105,
    109, 112, 108, 46, 90, 75, 83, 121, 110, 99, 77, 97, 112, 36, 75, 101, 121, 86, 97, 108, 117,
    101, 90, 158, 26, 0, 73, 105, 76, 122, 2, 0, 2, 76, 0, 3, 107, 101, 121, 116, 0, 18, 76, 106,
    97, 118, 97, 47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 76, 0, 5, 118, 97, 108,
    117, 101, 113, 0, 126, 0, 1, 120, 112, 116, 0, 36,
];
const MIDDLE_OF_DATA: [u8; 3] = [116, 0, 85];

impl ClusterManager for ZookeeperClusterManager {
    #[inline]
    fn add_sub(&self, address: String) {
        let is_subs = self
            .zookeeper
            .exists(&format!("{}/{}", ZK_PATH_SUBS, address), false);
        match is_subs {
            Ok(stat) => match stat {
                Some(_s) => {}
                None => {
                    let _ = self.zookeeper.create(
                        &format!("{}/{}", ZK_PATH_SUBS, address),
                        vec![],
                        Acl::open_unsafe().clone(),
                        CreateMode::Persistent,
                    );
                }
            },
            Err(e) => {
                error!("error on get subs: {:?}", e);
            }
        }

        let sub_name = format!(
            "{}:{}:{}",
            self.node_id, self.cluster_node.serverID.host, self.cluster_node.serverID.port
        );
        let mut oos = ObjectOutputStream::new();
        oos.write_object(&self.cluster_node);
        let result = self.zookeeper.create(
            &format!("{}/{}/{}", ZK_PATH_SUBS, address, sub_name),
            oos.to_byte_array(),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        );
        match result {
            Ok(r) => {
                debug!("Created node: {}", r);
            }
            Err(e) => {
                error!("cannot create node, error {:?}", e);
            }
        }
    }
    #[inline]
    fn set_cluster_node_info(&mut self, node: ClusterNodeInfo) {
        self.cluster_node = node;
        let result = self.zookeeper.create(
            &format!("{}/{}", ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH, &self.node_id),
            self.cluster_node.nodeId.clone().into_bytes(),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        );
        match result {
            Ok(r) => {
                debug!("Created node: {}", r);
            }
            Err(e) => {
                error!("cannot create node, error {:?}", e);
            }
        }

        let mut data: Vec<u8> = vec![];
        data.extend(&HEAD_OF_DATA);
        data.extend(self.node_id.as_bytes());
        data.extend(&MIDDLE_OF_DATA);
        data.extend(
            format!(
                "{}{}{}{}{}",
                "{\"verticles\":[],\"group\":\"__DISABLED__\",\"server_id\":{\"host\":\"",
                self.cluster_node.serverID.host.clone(),
                "\",\"port\":",
                self.cluster_node.serverID.port,
                "}}"
            )
            .as_bytes(),
        );

        let result = self.zookeeper.create(
            &format!("{}/{}", ZK_PATH_HA_INFO, &self.cluster_node.nodeId),
            data,
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        );
        match result {
            Ok(r) => {
                debug!("Created node: {}", r);
            }
            Err(e) => {
                error!("cannot create node, error {:?}", e);
            }
        }
    }
    #[inline]
    fn get_node_id(&self) -> String {
        self.node_id.clone()
    }
    #[inline]
    fn get_nodes(&self) -> Vec<String> {
        self.nodes.lock().unwrap().clone()
    }

    #[inline]
    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>> {
        self.ha_infos.clone()
    }

    #[inline]
    fn get_subs(&self) -> Arc<Mutex<MultiMap<String, ClusterNodeInfo>>> {
        self.subs.clone()
    }

    fn join(&mut self) {
        self.watch_nodes();
        self.watch_ha_info();
        self.watch_subs();
        info!("join node: {:?} to zookeeper cluster", self.node_id);
    }

    fn leave(&self) {
        unimplemented!()
    }

    #[inline]
    fn next(&self, len: usize) -> usize {
        let mut ci = self.cur_idx.write().unwrap();
        let idx = *ci;
        *ci = (*ci + 1) % len;
        return idx;
    }
}

impl ZookeeperClusterManager {
    fn watch_nodes(&mut self) {
        let mut nodes_cache =
            PathChildrenCache::new(self.zookeeper.clone(), ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH)
                .unwrap();
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
        nodes_cache.add_listener(move |event| match event {
            PathChildrenCacheEvent::ChildAdded(_id, data) => {
                nodes_clone
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(data.0.to_vec()).unwrap());
            }
            PathChildrenCacheEvent::ChildRemoved(id) => {
                let id = id.replace("/cluster/nodes/", "");
                let mut vec = nodes_clone.lock().unwrap();
                for (idx, uid) in vec.iter().enumerate() {
                    if id == uid.clone() {
                        vec.remove(idx);
                        break;
                    }
                }
            }
            _ => {}
        });
    }
}

impl ZookeeperClusterManager {
    fn watch_ha_info(&mut self) {
        let mut ha_info_cache =
            PathChildrenCache::new(self.zookeeper.clone(), ZK_PATH_HA_INFO).unwrap();
        match ha_info_cache.start() {
            Err(err) => {
                error!("error starting cache: {:?}", err);
                return;
            }
            _ => {
                debug!("{:?} started", ZK_PATH_HA_INFO);
            }
        }
        ha_info_cache.add_listener(move |event| match event {
            PathChildrenCacheEvent::ChildAdded(_id, data) => {
                let bytes_of_data = data.0.clone();
                if !bytes_of_data.is_empty() {
                    let bytes_as_string = String::from_utf8_lossy(&bytes_of_data);
                    let idx = bytes_as_string.rfind("$");
                    let bytes_as_string = &bytes_as_string[idx.unwrap() + 1..];
                    let mut bytes_as_string_split = bytes_as_string.split("t\u{0}U");
                    let key_value = ZKSyncMapKeyValue {
                        key: Object {
                            key: bytes_as_string_split.next().unwrap().to_string(),
                            value: bytes_as_string_split.next().unwrap().to_string(),
                        },
                    };

                    debug!("{:?}", key_value);
                }
            }
            PathChildrenCacheEvent::ChildRemoved(_) => {}
            _ => {}
        });
    }
}

impl ZookeeperClusterManager {
    fn watch_subs(&mut self) {
        let mut zk_path_subs =
            PathChildrenCache::new(self.zookeeper.clone(), ZK_PATH_SUBS).unwrap();
        zk_path_subs.start().unwrap();
        let subs: Arc<Mutex<Vec<Mutex<PathChildrenCache>>>> = Arc::new(Mutex::new(Vec::new()));
        let clone_subs = subs.clone();

        let clone_tcp_conns = self.tcp_conns.clone();

        let (sender, receiver): (
            std::sync::mpsc::Sender<String>,
            std::sync::mpsc::Receiver<String>,
        ) = std::sync::mpsc::channel();
        let zk_clone = self.zookeeper.clone();
        let subs_clone = self.subs.clone();
        std::thread::spawn(move || loop {
            match receiver.recv() {
                Ok(recv) => {
                    let inner_subs =
                        Mutex::new(PathChildrenCache::new(zk_clone.clone(), &recv).unwrap());
                    inner_subs.lock().unwrap().start().unwrap();

                    let inner_subs_clone = subs_clone.clone();
                    let inner_tcp_conns = clone_tcp_conns.clone();

                    inner_subs.lock().unwrap().add_listener(move |ev| {
                        let mut inner_tcp_conns0 = inner_tcp_conns.clone();
                        match ev {
                            PathChildrenCacheEvent::ChildAdded(_, data) => {
                                let msg_addr = recv.replace("/asyncMultiMap/__vertx.subs/", "");
                                let data_as_byte = &data.0;
                                let mut deser = ObjectInputStream {};
                                let node: ClusterNodeInfo = deser.read_object(data_as_byte.clone());
                                debug!("{:?}", node);
                                let mut i_subs = inner_subs_clone.lock().unwrap();
                                let node_id = node.nodeId.clone();
                                let host = node.serverID.host.clone();
                                let port = node.serverID.port;
                                i_subs.insert(msg_addr, node);

                                let tcp = RUNTIME
                                    .block_on(TcpStream::connect(format!("{}:{}", host, port)));
                                match tcp {
                                    Ok(tcp) => {
                                        let tcp_conn = Arc::new(tcp);
                                        unsafe {
                                            Arc::get_mut_unchecked(&mut inner_tcp_conns0)
                                                .insert(node_id, tcp_conn);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("cannot connect to server: {:?}", e);
                                    }
                                }
                            }
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
                            }
                            _ => {}
                        }
                    });

                    clone_subs.lock().unwrap().push(inner_subs);
                }
                Err(_) => {}
            }
        });

        let sender_clone = sender.clone();

        zk_path_subs.add_listener(move |event| match event {
            PathChildrenCacheEvent::ChildAdded(_id, _data) => {
                sender_clone.send(_id.clone()).unwrap();
            }
            _ => {}
        });
    }
}
