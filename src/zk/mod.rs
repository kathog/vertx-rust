use std::sync::{Arc, Mutex};
use jvm_serializable::java::io::*;
use serde::{Serialize, Deserialize};
use multimap::MultiMap;
use uuid::Uuid;
use zookeeper::{ZooKeeper, Acl};
use tokio::time::Duration;
use log::{error, info, LevelFilter, warn, debug};
use zookeeper::recipes::cache::{PathChildrenCache, PathChildrenCacheEvent};
use zookeeper::CreateMode;
use std::alloc::dealloc;
use std::collections::hash_map::RandomState;
use crate::vertx::{ClusterManager, Vertx, ClusterNodeInfo, VertxOptions, EventBus, RUNTIME};
use std::convert::TryInto;
use tokio::net::TcpStream;
use hashbrown::HashMap;
use std::sync::RwLock;

#[cfg(test)]
mod tests {
    use crate::zk::ZookeeperClusterManager;
    use simple_logger::SimpleLogger;
    use tokio::time::Duration;
    use std::sync::Arc;
    use crate::vertx::{ClusterManager, Vertx, ClusterNodeInfo, VertxOptions, EventBus};
    use log::{error, info, debug};
    use crate::net::NetServer;


    #[test]
    fn zk_vertx() {
        SimpleLogger::new().init().unwrap();

        let mut vertx_options = VertxOptions::default();
        debug!("{:?}", vertx_options);
        let mut vertx : Vertx<ZookeeperClusterManager> = Vertx::new(vertx_options);
        let zk = ZookeeperClusterManager::new("127.0.0.1:2181".to_string(), "io.vertx.01".to_string());
        vertx.set_cluster_manager(zk);
        let event_bus : &'static mut Arc<EventBus<ZookeeperClusterManager>> = Box::leak(Box::new(vertx.event_bus()));
        
        event_bus.consumer("test.01", move |m| {
            let body = m.body();

            // info!("consumer {:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());

            m.reply(format!("response => {}", std::str::from_utf8(&body).unwrap()).as_bytes().to_vec());
        });
        std::thread::sleep(Duration::from_secs(1));
        let time = std::time::Instant::now();

        let mut net_server = NetServer::new(Some(event_bus.clone()));
        net_server.listen(9091, move |req, ev| {
            let mut resp = vec![];

            ev.request_with_callback("test.01", format!("regest:"), move |m| {
                let _body = m.body();
                // COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // info!("set_callback_function {:?}, thread: {:?}", std::str::from_utf8(&_body), std::thread::current().id());
            });

            let data = r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: 14

{"code": "UP"}
"#.to_string();

            resp.extend_from_slice(data.as_bytes());
            resp
        });
        
        let elapsed = time.elapsed();
        std::thread::park();
        // println!("count {:?}, time: {:?}", COUNT.load(std::sync::atomic::Ordering::SeqCst), &elapsed);
        // println!("avg time: {:?} ns", (&elapsed.as_nanos() / COUNT.load(std::sync::atomic::Ordering::SeqCst) as u128));

    }

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
    zookeeper: Arc<ZooKeeper>,
    cluster_node: ClusterNodeInfo,
    tcp_conns: Arc<HashMap<String, Arc<TcpStream>>>,
    cur_idx: Arc<RwLock<usize>>,

}

impl ZookeeperClusterManager {

    pub fn new (zk_hosts: String, zk_root: String) -> ZookeeperClusterManager {
        let zookeeper = ZooKeeper::connect(&format!("{}/{}", zk_hosts, zk_root), Duration::from_secs(1), |x| {}).unwrap();

        ZookeeperClusterManager {
            nodes : Arc::new(Mutex::new(Vec::new())),
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

const head_of_data : [u8; 119] =  [0, 172, 237, 0, 5, 115, 114, 0, 54, 105, 111, 46, 118, 101, 114, 116, 120, 46, 115, 112, 105, 46, 99, 108, 117, 115, 116, 101, 114, 46, 122, 111, 111, 107, 101, 101, 112, 101, 114, 46, 105, 109, 112, 108, 46, 90, 75, 83, 121, 110, 99, 77, 97, 112, 36, 75, 101, 121, 86, 97, 108, 117, 101, 90, 158, 26, 0, 73, 105, 76, 122, 2, 0, 2, 76, 0, 3, 107, 101, 121, 116, 0, 18, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 76, 0, 5, 118, 97, 108, 117, 101, 113, 0, 126, 0, 1, 120, 112, 116, 0, 36];
const middle_of_data : [u8; 3] = [116, 0, 85];

impl ClusterManager for ZookeeperClusterManager {


    fn add_sub(&self, address: String) {
        let is_subs = self.zookeeper.exists(&format!("{}/{}", ZK_PATH_SUBS, address), false);
        match is_subs {
            Ok(stat) => {
                match stat {
                    Some(_s) => {},
                    None => {
                        self.zookeeper.create(&format!("{}/{}", ZK_PATH_SUBS, address),
                                              vec![],
                                              Acl::open_unsafe().clone(),
                                              CreateMode::Persistent);
                    }
                }
            },
            Err(e) => {
                error!("error on get subs: {:?}", e);
            }
        }

        let sub_name = format!("{}:{}:{}", self.node_id, self.cluster_node.serverID.host, self.cluster_node.serverID.port);
        let mut oos = ObjectOutputStream::new();
        oos.write_object(&self.cluster_node);
        let result = self.zookeeper.create(&format!("{}/{}/{}", ZK_PATH_SUBS, address, sub_name),
                                           oos.to_byte_array(),
                                           Acl::open_unsafe().clone(),
                                           CreateMode::Ephemeral);
        match result {
            Ok(r) => {
                debug!("Created node: {}", r);
            },
            Err(e) => {
                error!("cannot create node, error {:?}", e);
            }
        }
    }

    fn set_cluster_node_info(&mut self, node: ClusterNodeInfo) {
        self.cluster_node = node;
        let result = self.zookeeper.create(&format!("{}/{}", ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH, &self.node_id),
                                           self.cluster_node.nodeId.clone().into_bytes(),
                                           Acl::open_unsafe().clone(),
                                           CreateMode::Ephemeral);
        match result {
            Ok(r) => {
                debug!("Created node: {}", r);
            },
            Err(e) => {
                error!("cannot create node, error {:?}", e);
            }
        }


        let mut data: Vec<u8> = vec![];
        data.extend(&head_of_data);
        data.extend(self.node_id.as_bytes());
        data.extend(&middle_of_data);
        data.extend(format!("{}{}{}{}{}","{\"verticles\":[],\"group\":\"__DISABLED__\",\"server_id\":{\"host\":\"",
                            self.cluster_node.serverID.host.clone(),
                            "\",\"port\":",
                            self.cluster_node.serverID.port,
                            "}}").as_bytes());

        let result = self.zookeeper.create(&format!("{}/{}", ZK_PATH_HA_INFO, &self.cluster_node.nodeId),
                                           data,
                                           Acl::open_unsafe().clone(),
                                           CreateMode::Ephemeral);
        match result {
            Ok(r) => {
                debug!("Created node: {}", r);
            },
            Err(e) => {
                error!("cannot create node, error {:?}", e);
            }
        }

    }

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

    // fn get_conn(&self, node_id: &String) -> Option<Arc<TcpStream>> {
    //     return match self.tcp_conns.get(node_id) {
    //         Some(conn) => Some(conn.clone()),
    //         None => None
    //     }
    // }

    fn join(&mut self) {
        self.watch_nodes();
        self.watch_ha_info();
        self.watch_subs();
    }

    fn leave(&self) {
        unimplemented!()
    }

    fn next(&self, len: usize) -> usize {
        let mut ci = self.cur_idx.write().unwrap();
        let idx = *ci;
        *ci = (*ci + 1) % len;
        return idx;
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
                    nodes_clone.lock().unwrap().push(String::from_utf8(data.0.to_vec()).unwrap());
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
                    if !bytes_of_data.is_empty() {
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
                    }
                },
                PathChildrenCacheEvent::ChildRemoved(id) => {

                }
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

        let clone_tcp_conns = self.tcp_conns.clone();

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
                        let inner_tcp_conns = clone_tcp_conns.clone();

                        inner_subs.lock().unwrap().add_listener(move |ev| {
                            let mut inner_tcp_conns0 = inner_tcp_conns.clone();
                            match ev {
                                PathChildrenCacheEvent::ChildAdded(id, data) => {
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

                                    let tcp = RUNTIME.block_on(TcpStream::connect(format!("{}:{}", host, port)));
                                    match tcp {
                                        Ok(tcp) => {

                                            let tcp_conn = Arc::new(tcp);
                                            unsafe {
                                                Arc::get_mut_unchecked(&mut inner_tcp_conns0).insert(node_id, tcp_conn);
                                            }
                                        },
                                        Err(e) => {
                                            warn!("cannot connect to server: {:?}", e);
                                        }
                                    }

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
