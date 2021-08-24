use std::sync::{Arc, Mutex, RwLock};
use crate::vertx::cm::{ClusterNodeInfo, ClusterManager, ServerID};
use multimap::MultiMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use crossbeam_channel::{bounded, Sender, Receiver};
use std::time::Duration;

#[repr(C)]
#[derive(Debug, Clone)]
pub struct CServerID {
    host: * const c_char,
    port: i32
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct CClusterNodeInfo {
    node_id: *const c_char,
    server_id: CServerID
}

impl From<&ClusterNodeInfo> for CClusterNodeInfo {
    fn from(node: &ClusterNodeInfo) -> Self {
        CClusterNodeInfo {
            node_id: node.nodeId.as_ptr() as *const c_char,
            server_id: CServerID {
                host: node.serverID.host.as_ptr() as *const c_char,
                port: node.serverID.port
            }
        }
    }
}

impl Into<ClusterNodeInfo> for CClusterNodeInfo {
    fn into(self) -> ClusterNodeInfo {
        unsafe {
            ClusterNodeInfo {
                nodeId: CStr::from_ptr(self.node_id).to_str().unwrap().to_string(),
                serverID: ServerID {
                    port: self.server_id.port,
                    host: CStr::from_ptr(self.server_id.host).to_str().unwrap().to_string()
                }
            }
        }
    }
}

#[link(name = "vertx_rust")]
extern "C" {
    pub(crate) fn join(port: i32, host: *const c_char);
    pub(crate) fn add_sub(address: *const c_char, node: &CClusterNodeInfo);
}

#[no_mangle]
pub extern "C" fn refresh_subs () {
    println!("invoke refresh_subs()");
    let _ = refresh_channel.0.send(true);
}

lazy_static! {
    static ref refresh_channel : (Sender<bool>, Receiver<bool>) = bounded(1);
}

pub struct HazelcastClusterManager {
    node_id: String,
    nodes: Arc<Mutex<Vec<String>>>,
    ha_infos: Arc<Mutex<Vec<ClusterNodeInfo>>>,
    subs: Arc<RwLock<MultiMap<String, ClusterNodeInfo>>>,
    cluster_node: ClusterNodeInfo,
    cur_idx: Arc<RwLock<usize>>,
}

impl HazelcastClusterManager {

    pub fn new() -> HazelcastClusterManager {
        let node_id = uuid::Uuid::new_v4().to_string();
        HazelcastClusterManager {
            node_id: node_id.to_string(),
            nodes: Arc::new(Mutex::new(vec![])),
            ha_infos: Arc::new(Mutex::new(vec![])),
            subs: Arc::new(Default::default()),
            cluster_node: ClusterNodeInfo {
                nodeId: node_id,
                serverID: ServerID {
                    port: 0,
                    host: "127.0.0.1".to_string()
                }
            },
            cur_idx: Arc::new(Default::default())
        }
    }

}

impl ClusterManager for HazelcastClusterManager {

    #[inline]
    fn add_sub(&self, address: String) {
        unsafe {
            add_sub(address.as_ptr() as *const c_char, &CClusterNodeInfo::from(&self.cluster_node));
        }
    }

    fn set_cluster_node_info(&mut self, node: ClusterNodeInfo) {
        todo!()
    }

    fn get_node_id(&self) -> String {
        todo!()
    }

    fn get_nodes(&self) -> Vec<String> {
        todo!()
    }

    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>> {
        todo!()
    }

    fn get_subs(&self) -> Arc<RwLock<MultiMap<String, ClusterNodeInfo>>> {
        todo!()
    }

    fn join(&mut self) {
        unsafe {
            let host = CString::from_vec_unchecked(b"localhost".to_vec());
            join(5701, host.as_ptr());
        }
        std::thread::spawn(|| {
           loop {
               let _ = refresh_channel.1.recv();
               // refresh subs
           }
        });

        std::thread::sleep(Duration::from_secs(1));
        self.add_sub("dsds".to_string());
    }

    fn leave(&self) {
        todo!()
    }

    fn next(&self, len: usize) -> usize {
        todo!()
    }
}