use jvm_serializable::java::io::*;
use multimap::MultiMap;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::RandomState;
use std::sync::{Arc, Mutex, RwLock};

//Struct represented information about vertx node server
#[jvm_object(io.vertx.core.net.impl.ServerID,5636540499169644934)]
pub struct ServerID {
    pub port: i32,
    pub host: String,
}

//Struct represented information about vertx node
#[jvm_object(io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo,1)]
pub struct ClusterNodeInfo {
    pub nodeId: String,
    pub serverID: ServerID,
}

//Interface of cluster manager support integrations of cluster nodes
pub trait ClusterManager: Send + Clone {
    //Register current node as vertx sub
    fn add_sub(&self, address: String);

    //Register current node as vertx node in cluster
    fn set_cluster_node_info(&mut self, node: ClusterNodeInfo);

    //Get uniquie node id in cluster
    fn get_node_id(&self) -> String;

    //Get id list of all nodes in cluster
    fn get_nodes(&self) -> Vec<String>;

    //Get list of all nodes in cluster
    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>>;

    //Get all registered subs and nodes in this subs
    fn get_subs(&self) -> Arc<RwLock<MultiMap<String, ClusterNodeInfo>>>;

    //Join current node to vertx cluster
    fn join(&mut self);

    //Leave current cluster from node
    fn leave(&self);

    //Round rubin index of nodes
    fn next(&self, len: usize) -> usize;
}

//Empty implementation of cluster manager to create vertx standalone instance
#[derive(Clone)]
pub struct NoClusterManager;

impl ClusterManager for NoClusterManager {
    fn add_sub(&self, _address: String) {
        unimplemented!()
    }

    fn set_cluster_node_info(&mut self, _node: ClusterNodeInfo) {
        unimplemented!()
    }

    fn get_node_id(&self) -> String {
        unimplemented!()
    }

    fn get_nodes(&self) -> Vec<String> {
        unimplemented!()
    }

    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>> {
        unimplemented!()
    }

    fn get_subs(&self) -> Arc<RwLock<MultiMap<String, ClusterNodeInfo, RandomState>>> {
        unimplemented!()
    }

    fn join(&mut self) {
        unimplemented!()
    }

    fn leave(&self) {
        unimplemented!()
    }

    fn next(&self, _len: usize) -> usize {
        unimplemented!()
    }
}
