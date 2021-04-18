use std::sync::{Arc, Mutex, RwLock};
use crate::vertx::cm::{ClusterNodeInfo, ClusterManager, ServerID};
use multimap::MultiMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

#[link(name = "vertx_rust")]
extern "C" {
    pub(crate) fn join(port: i32, host: *const c_char);

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

    pub fn new() -> HazelcastClusterManager{
        HazelcastClusterManager {
            node_id: "".to_string(),
            nodes: Arc::new(Mutex::new(vec![])),
            ha_infos: Arc::new(Mutex::new(vec![])),
            subs: Arc::new(Default::default()),
            cluster_node: ClusterNodeInfo {
                nodeId: "".to_string(),
                serverID: ServerID {
                    port: 0,
                    host: "".to_string()
                }
            },
            cur_idx: Arc::new(Default::default())
        }
    }

}

impl ClusterManager for HazelcastClusterManager {
    fn add_sub(&self, address: String) {
        todo!()
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
            let host = CString::from_vec_unchecked(b"127.0.0.1".to_vec());
            join(7001, host.as_ptr());
        }
    }

    fn leave(&self) {
        todo!()
    }

    fn next(&self, len: usize) -> usize {
        todo!()
    }
}