use core::fmt::Debug;
// use rayon::prelude::*;
use rayon::{ThreadPoolBuilder, ThreadPool};
use std::collections::HashMap;
use std::{
    sync::{
        Arc,
        mpsc::{
            channel,
            Sender,
            Receiver,
        },
        Mutex,
    },
    thread::JoinHandle,
    panic::*,
};
use log::{info, debug, trace, warn};
use multimap::MultiMap;
use jvm_serializable::java::io::*;
use serde::{Serialize, Deserialize};
use std::collections::hash_map::RandomState;
use serde::export::PhantomData;
use rand::thread_rng;
use rand::Rng;
use std::sync::Once;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use std::convert::TryInto;
use crate::net;


static EV_INIT: Once = Once::new();

lazy_static! {
    pub static ref RUNTIME: Runtime = {
        Runtime::new().unwrap()
    };
}

#[jvm_object(io.vertx.core.net.impl.ServerID,5636540499169644934)]
pub struct ServerID {
    pub port: i32,
    pub host: String
}


#[jvm_object(io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo,1)]
pub struct ClusterNodeInfo {
    pub nodeId: String,
    pub serverID: ServerID,
}


pub trait ClusterManager {

    fn add_sub(&self, address: String);

    fn set_cluster_node_info(&mut self, node: ClusterNodeInfo);

    fn get_node_id(&self) -> String;

    fn get_nodes(&self) -> Vec<String>;

    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>>;

    fn get_subs(&self) -> Arc<Mutex<MultiMap<String, ClusterNodeInfo>>>;

    fn join(&mut self);

    fn leave(&self);

}

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

    fn get_subs(&self) -> Arc<Mutex<MultiMap<String, ClusterNodeInfo, RandomState>>> {
        unimplemented!()
    }

    fn join(&mut self) {
        unimplemented!()
    }

    fn leave(&self) {
        unimplemented!()
    }
}


#[derive(Debug, Clone)]
pub struct VertxOptions {
    worker_pool_size : usize,
    vertx_host : String,
    vertx_port : u16,
    event_bus_options : EventBusOptions,
}



impl Default for VertxOptions {
    fn default() -> Self {
        let cpus = num_cpus::get();
        let vertx_port: u16 = 0;
        let vertx_host = "127.0.0.1".to_owned();
        VertxOptions {
            worker_pool_size : cpus/2,
            vertx_host : vertx_host.clone(),
            vertx_port,
            event_bus_options: EventBusOptions::from((vertx_host, vertx_port)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventBusOptions {

    event_bus_pool_size: usize,
    vertx_host : String,
    vertx_port : u16,
}

impl From<(String, u16)> for EventBusOptions {

    fn from(opts: (String, u16)) -> Self {
        let cpus = num_cpus::get();
        EventBusOptions {
            event_bus_pool_size : cpus/2,
            vertx_host : opts.0,
            vertx_port : opts.1,
        }
    }
}

impl Default for EventBusOptions {
    fn default() -> Self {
        let cpus = num_cpus::get();
        let vertx_port: u16 = 0;
        EventBusOptions {
            event_bus_pool_size : cpus/2,
            vertx_host : String::from("127.0.0.1"),
            vertx_port,
            // cluster_manager: None
        }
    }
}

#[derive(Clone, Default)]
pub struct Message {
    address: Option<String>,
    replay: Option<String>,
    body: Arc<Vec<u8>>,
    protocol_version: i32,
    system_codec_id: i32,
    send: bool,
    port: i32,
    host: String,
    headers: i32,
    request: bool,
    local: bool
}

impl Message {

    pub fn body (&self) -> Arc<Vec<u8>> {
        return self.body.clone();
    }

    pub fn reply(&self, mut data: Vec<u8>) {
        unsafe {
            let mut clone_body = self.body.clone();
            let inner_body = Arc::get_mut_unchecked(&mut clone_body);
            inner_body.clear();
            inner_body.append(&mut data);
        }
    }
}

impl From<Vec<u8>> for Message {
    fn from(msg: Vec<u8>) -> Self {
        Message {
            ..Default::default()
        }
    }
}

impl Message {

    pub fn to_vec(&self) -> Result<Vec<u8>, &str> {
        let mut data = vec![];
        data.push(1);
        data.push(12);
        data.push(0);
        let address = self.address.clone().expect("Replay message not found!");
        data.extend_from_slice(&(address.len() as i32).to_be_bytes());
        data.extend_from_slice(address.as_bytes());
        match self.replay.clone() {
            Some(addr) => {
                data.extend_from_slice(&(addr.len() as i32).to_be_bytes());
                data.extend_from_slice(addr.as_bytes());
            }, 
            None => {
                data.extend_from_slice(&(0 as i32).to_be_bytes());
            }
        }    
        data.extend_from_slice(&self.port.to_be_bytes());
        data.extend_from_slice(&(self.host.len() as i32).to_be_bytes());
        data.extend_from_slice(self.host.as_bytes());
        data.extend_from_slice(&(4 as i32).to_be_bytes());
        data.extend_from_slice(&(self.body.len() as i32).to_be_bytes());
        data.extend_from_slice(self.body.as_slice());
        let len = ((data.len()) as i32).to_be_bytes();
        for idx in 0..4 {
            data.insert(idx, len[idx]);
        }
        return Ok(data);
    }
}

impl Debug for Message {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("address", &self.address)
            .field("replay", &self.replay)
            .field("body", &self.body)
            .finish()
    }
}


pub struct Vertx<CM:'static + ClusterManager + Send + Sync> {
    options : VertxOptions,
    worker_pool: ThreadPool,
    event_bus: Arc<EventBus<CM>>,
    ph: PhantomData<CM>
}

impl <CM:'static + ClusterManager + Send + Sync>Vertx<CM> {

    pub fn new (options: VertxOptions) -> Vertx<CM> {
        let worker_pool = ThreadPoolBuilder::new().num_threads(options.worker_pool_size).build().unwrap();
        let event_bus = EventBus::<CM>::new(options.event_bus_options.clone());
        return Vertx {
            options,
            worker_pool,
            event_bus : Arc::new(event_bus),
            ph: PhantomData,
        };
    }

    pub fn set_cluster_manager(&mut self, cm: CM) {
        debug!("set_cluster_manager: {:?}", cm.get_nodes());
        Arc::get_mut(&mut self.event_bus).unwrap().set_cluster_manager(cm);
    }


    pub fn start(&self) {
        
    }

    pub fn event_bus(&self) -> Arc<EventBus<CM>> {
        EV_INIT.call_once(|| {
            let mut ev = self.event_bus.clone();
            unsafe {
                let opt_ev = Arc::get_mut_unchecked(&mut ev);
                opt_ev.init();
            }
        });
        return self.event_bus.clone();
    }
}

pub struct EventBus<CM:'static + ClusterManager + Send + Sync> {
    options : EventBusOptions,
    event_bus_pool: Arc<ThreadPool>,
    consumers: Arc<HashMap<String, Box<dyn Fn(&mut Message) + Send + Sync>>>,
    callback_functions: Arc<Mutex<HashMap<String, Box<dyn Fn(&Message) + Send + Sync + UnwindSafe>>>>,
    sender: Mutex<Sender<Message>>,
    receiver_joiner : Arc<JoinHandle<()>>,
    cluster_manager: Arc<Option<CM>>,
    event_bus_port: u16,
}

impl <CM:'static + ClusterManager + Send + Sync>EventBus<CM> {

    pub fn new (options: EventBusOptions) -> EventBus<CM> {
        let event_bus_pool = ThreadPoolBuilder::new().num_threads(options.event_bus_pool_size).build().unwrap();
        let (sender, _receiver) : (Sender<Message>, Receiver<Message>) = channel();
        let receiver_joiner = std::thread::spawn(||{});
        let ev = EventBus {
            options,
            event_bus_pool : Arc::new(event_bus_pool),
            consumers: Arc::new(HashMap::new()),
            callback_functions: Arc::new(Mutex::new(HashMap::new())),
            sender : Mutex::new(sender),
            receiver_joiner : Arc::new(receiver_joiner),
            cluster_manager: Arc::new(None),
            event_bus_port: 0,
        };
        return ev;
    }

    fn set_cluster_manager(&mut self, cm: CM) {
        let mut m = cm;
        m.join();
        self.cluster_manager = Arc::new(Some(m));    
    }

    fn start (&mut self) {
        let joiner = &self.receiver_joiner;
        let h = joiner.clone();
        unsafe {
            let val :JoinHandle<()> = std::ptr::read(&*h);
            val.join().unwrap();
        }
    }

    fn init(&mut self) {
        let (sender, receiver) : (Sender<Message>, Receiver<Message>) = channel();
        self.sender = Mutex::new(sender);
        let local_consumers = self.consumers.clone();
        let local_cf = self.callback_functions.clone();
        let pool = self.event_bus_pool.clone();
        let local_sender = self.sender.lock().unwrap().clone();
        

        let mut net_server = net::NetServer::new();
        net_server.listen_for_message(self.options.vertx_port, |req| {
            let resp = vec![];
            info!("{:?}", String::from_utf8_lossy(req));
            return resp;
        });

        self.event_bus_port = net_server.port;
        let opt_cm = Arc::get_mut(&mut self.cluster_manager).unwrap();
        match opt_cm {
            Some(cm) => {
                cm.set_cluster_node_info(ClusterNodeInfo {
                    nodeId : cm.get_node_id(),
                    serverID: ServerID {
                        host: self.options.vertx_host.clone(),
                        port: self.event_bus_port as i32
                    }
                });
            },
            None => {}
        }
        info!("start event bus on: {}:{}", self.options.vertx_host, self.event_bus_port);

        let local_cm = self.cluster_manager.clone();
        let joiner = std::thread::spawn(move || -> (){
            loop {
                match receiver.recv() {
                    Ok(msg) => {
                        trace!("{:?}", msg);
                        let inner_consummers = local_consumers.clone();
                        let inner_cf = local_cf.clone();
                        let inner_sender = local_sender.clone();
                        let mut inner_cm = local_cm.clone();
                        
                        pool.spawn(move || {
                            let mut mut_msg = msg;
                            match &mut_msg.address {
                                Some(address) => {
                                    debug!("msg: {:?}", address);
                                    // invoke function from consumer
                                    let manager = unsafe { Arc::get_mut_unchecked(&mut inner_cm) };
                                    match manager {
                                        // ClusterManager
                                        Some(cm) => {
                                            debug!("manager: {:?}", cm.get_subs().lock().unwrap().len());
                                            let subs = cm.get_subs();
                                            let nodes = subs.lock().unwrap();
                                            let nodes_lock = nodes.get_vec(address);
                                            match nodes_lock {
                                                Some(n) => {
                                                    if n.len() == 0 {
                                                        warn!("subs not found");
                                                    } else {
                                                        let mut rng = thread_rng();
                                                        let idx: usize = rng.gen_range(0, n.len());
                                                        let node = &n[idx];
                                                        let host = node.serverID.host.clone();
                                                        let port = node.serverID.port.clone();
                                                        debug!("{:?}", node);
                                                        RUNTIME.spawn(async move {
                                                            match TcpStream::connect(format!("{}:{}", host, port)).await {
                                                                Ok(stream) => {
                                                                    debug!("{:?}", stream);
                                                                    let mut stream = stream;
                                                                    match stream.write_all(&mut_msg.to_vec().unwrap()).await {
                                                                        Ok(_r) => {
                                                                            let mut response = [0u8;1];
                                                                            let _ = stream.read(&mut response);
                                                                        },
                                                                        Err(e) => {
                                                                            warn!("Error in send message: {:?}", e);
                                                                        }
                                                                    }
                                                                },
                                                                Err(e) => {
                                                                    warn!("Error in send message: {:?}", e);
                                                                }
                                                            }
                                                        });        
                                                    }
                                                },
                                                None => {}
                                            }

                                        },
                                        None => {
                                            // NoClusterManager
                                            let callback = inner_consummers.get(address);
                                            debug!("manager not found");
                                            match callback {
                                                Some(caller) => {
                                                    caller.call((&mut mut_msg,));
                                                    mut_msg.address = None;
                                                    inner_sender.send(mut_msg).unwrap();
                                                },
                                                None => {}
                                            }
                                        }
                                    }
                                },
                                None => {
                                    // invoke callback function from message
                                    let address = mut_msg.replay.clone().unwrap();
                                    let callback = inner_cf.lock().unwrap().remove(&address);
                                    match callback {
                                        Some(caller) => {
                                            caller.call((&mut_msg,));
                                        },
                                        None => {}
                                    }
                                }
                            }
                        });

                    },
                    Err(_err) => {
                        println!("{:?}", _err);
                    }
                }
            }
        });
        self.receiver_joiner = Arc::new(joiner);
    }

    pub fn consumer<OP> (&self, address: &str,  op: OP)
        where OP : Fn(&mut Message,) + Send + 'static + Sync, {
        unsafe {
            let mut local_cons = self.consumers.clone();
            Arc::get_mut_unchecked(&mut local_cons).insert(address.to_string(), Box::new(op));
        }
        match self.cluster_manager.as_ref() {
            Some(cm) => {
                cm.add_sub(address.to_string());
            },
            None => {}
        };
    }

    pub fn request(&self, address: &str, request: String) {
        let addr = address.to_owned();
        let body = request.as_bytes().to_vec();
        let message = Message {
            address: Some(addr.clone()),
            replay: None,
            body: Arc::new(body),
            ..Default::default()
        };
        let local_sender = self.sender.lock().unwrap().clone();
        local_sender.send(message).unwrap();
    }

    pub fn request_with_callback<OP> (&self, address: &str, request: String, op: OP)
        where OP : Fn(& Message,) + Send + 'static + Sync + UnwindSafe, {
        let addr = address.to_owned();
        let body0 = request.as_bytes().to_vec();
        let message = Message {
            address: Some(addr.clone()),
            replay: Some(format!("__vertx.reply.{}", uuid::Uuid::new_v4().to_string())),
            body: Arc::new(body0),
            host: self.options.vertx_host.clone(),
            port: self.event_bus_port as i32,
            ..Default::default()
        };
        let local_cons = self.callback_functions.clone();
        local_cons.lock().unwrap().insert(message.replay.clone().unwrap(), Box::new(op));
        let local_sender = self.sender.lock().unwrap().clone();
        local_sender.send(message).unwrap();
    }

}