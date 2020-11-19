use core::fmt::Debug;
// use rayon::prelude::*;
use rayon::{ThreadPoolBuilder, ThreadPool};
use std::collections::HashMap;
use std::{
    sync::{
        Arc,
        Mutex,
    },
    thread::JoinHandle,
    panic::*,
};
use log::{info, debug, trace, warn, error};
use multimap::MultiMap;
use jvm_serializable::java::io::*;
use serde::{Serialize, Deserialize};
use std::collections::hash_map::RandomState;
use serde::export::PhantomData;
use std::sync::Once;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use std::convert::TryInto;
use crate::net;
use crossbeam_channel::*;
use crate::net::NetServer;
use std::net::Shutdown;
use std::ops::Deref;


static EV_INIT: Once = Once::new();

lazy_static! {
    pub static ref RUNTIME: Runtime = {
        Runtime::new().unwrap()
    };

    static ref TCPS : Arc<HashMap<String, Arc<TcpStream>>> = Arc::new(HashMap::new());
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


pub trait ClusterManager: Send {

    fn add_sub(&self, address: String);
    #[inline]
    fn set_cluster_node_info(&mut self, node: ClusterNodeInfo);
    #[inline]
    fn get_node_id(&self) -> String;
    #[inline]
    fn get_nodes(&self) -> Vec<String>;
    #[inline]
    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>>;
    #[inline]
    fn get_subs(&self) -> Arc<Mutex<MultiMap<String, ClusterNodeInfo>>>;

    // fn get_conn(&self, node_id: &String) -> Option<Arc<TcpStream>>;

    fn join(&mut self);

    fn leave(&self);

    #[inline]
    fn next(&self, len: usize) -> usize;

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

    // fn get_conn(&self, _node_id: &String) -> Option<Arc<TcpStream>> {
    //     unimplemented!()
    // }

    fn join(&mut self) {
        unimplemented!()
    }

    fn leave(&self) {
        unimplemented!()
    }

    fn next(&self, len: usize) -> usize {
        unimplemented!()
    }
}


#[derive(Debug, Clone)]
pub struct VertxOptions {
    worker_pool_size : usize,
    vertx_host : String,
    vertx_port : u16,
    pub event_bus_options : EventBusOptions,
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

    pub event_bus_pool_size: usize,
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

#[derive(Clone, Default, Debug)]
pub struct Message {
    address: Option<String>,
    replay: Option<String>,
    body: Arc<Vec<u8>>,
    protocol_version: i32,
    system_codec_id: i32,
    port: i32,
    host: String,
    headers: i32,
}

impl Message {

    #[inline]
    pub fn body (&self) -> Arc<Vec<u8>> {
        return self.body.clone();
    }

    #[inline]
    pub fn reply(&mut self, mut data: Vec<u8>) {
        unsafe {
            let mut clone_body = self.body.clone();
            let inner_body = Arc::get_mut_unchecked(&mut clone_body);
            inner_body.clear();
            inner_body.append(&mut data);
            self.address = self.replay.clone();
            self.replay = None;
        }
    }
}

impl From<Vec<u8>> for Message {
    #[inline]
    fn from(msg: Vec<u8>) -> Self {
        let mut idx = 3;
        let len_addr = i32::from_be_bytes(msg[idx..idx+4].try_into().unwrap()) as usize;
        idx += 4;
        let address = String::from_utf8(msg[idx..idx+len_addr].to_vec()).unwrap();
        idx += len_addr;
        let len_replay = i32::from_be_bytes(msg[idx..idx+4].try_into().unwrap()) as usize;
        idx += 4;
        let mut replay = None;
        if len_replay > 0 {
            let replay_str = String::from_utf8(msg[idx..idx+len_addr].to_vec()).unwrap();
            idx += len_replay;
            replay = Some(replay_str);
        }
        let port = i32::from_be_bytes(msg[idx..idx+4].try_into().unwrap());
        idx += 4;
        let len_host = i32::from_be_bytes(msg[idx..idx+4].try_into().unwrap()) as usize;
        idx += 4;
        let host = String::from_utf8(msg[idx..idx+len_host].to_vec()).unwrap();
        idx += len_host;
        let headers = i32::from_be_bytes(msg[idx..idx+4].try_into().unwrap());
        idx += 4;
        let len_body = i32::from_be_bytes(msg[idx..idx+4].try_into().unwrap()) as usize;
        idx += 4;
        let body = msg[idx..idx+len_body].to_vec();
        Message {
            address: Some(address.to_string()),
            replay,
            port,
            host,
            headers,
            body: Arc::new(body),
            ..Default::default()
        }
    }
}

impl Message {

    #[inline]
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
    consumers: Arc<HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>,) + Send + Sync>>>,
    callback_functions: Arc<Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>,) + Send + Sync + UnwindSafe>>>>,
    pub(crate) sender: Mutex<Sender<Message>>,
    receiver_joiner : Arc<JoinHandle<()>>,
    cluster_manager: Arc<Option<CM>>,
    event_bus_port: u16,
    self_arc: Option<Arc<EventBus<CM>>>,

}

impl <CM:'static + ClusterManager + Send + Sync>EventBus<CM> {

    pub fn new (options: EventBusOptions) -> EventBus<CM> {
        let event_bus_pool = ThreadPoolBuilder::new().num_threads(options.event_bus_pool_size).build().unwrap();
        let (sender, _receiver) : (Sender<Message>, Receiver<Message>) = unbounded();
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
            self_arc: None
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
        let (sender, receiver) : (Sender<Message>, Receiver<Message>) = unbounded();
        self.sender = Mutex::new(sender);
        let local_consumers = self.consumers.clone();
        let local_cf = self.callback_functions.clone();
        let pool = self.event_bus_pool.clone();
        let local_sender = self.sender.lock().unwrap().clone();
        self.self_arc = Some(unsafe { Arc::from_raw(self) });

        let net_server = self.create_net_server();
        self.event_bus_port = net_server.port;
        self.registry_in_cm();
        info!("start event bus on: {}:{}", self.options.vertx_host, self.event_bus_port);

        self.prepare_consumer_msg(receiver, local_consumers, local_cf, pool, local_sender);
    }

    fn prepare_consumer_msg(&mut self, receiver: Receiver<Message>, local_consumers: Arc<HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>) + Send + Sync>>>,
                            local_cf: Arc<Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>) + Send + Sync + UnwindSafe>>>>,
                            pool: Arc<ThreadPool>,
                            local_sender: Sender<Message>) {
        let mut local_cm = self.cluster_manager.clone();
        let local_ev = self.self_arc.clone();

        let joiner = std::thread::spawn(move || -> (){
            loop {
                match receiver.recv() {
                    Ok(msg) => {
                        trace!("{:?}", msg);
                        let inner_consummers = local_consumers.clone();
                        let inner_cf = local_cf.clone();
                        let inner_sender = local_sender.clone();
                        let mut inner_cm = local_cm.clone();
                        let inner_ev = local_ev.clone();


                        pool.spawn(move || {
                            let mut mut_msg = msg;
                            match mut_msg.address.clone() {
                                Some(address) => {
                                    // invoke function from consumer
                                    let mut inner_cm0 = inner_cm.clone();
                                    let manager = unsafe { Arc::get_mut_unchecked(&mut inner_cm0) };
                                    match manager {
                                        // ClusterManager
                                        Some(cm) => {
                                            debug!("manager: {:?}", cm.get_subs().lock().unwrap().len());
                                            let subs = cm.get_subs();
                                            let nodes = subs.lock().unwrap();
                                            let nodes_lock = nodes.get_vec(&address.clone());

                                            match nodes_lock {
                                                Some(n) => {
                                                    if n.len() == 0 {
                                                        warn!("subs not found");
                                                    } else {
                                                        let idx = cm.next(n.len());
                                                        let mut node = n.get(idx);
                                                        match node {
                                                            Some(_) => {},
                                                            None => {
                                                                let idx = cm.next(n.len());
                                                                node = n.get(idx);
                                                            }
                                                        }
                                                        let node = node.unwrap();
                                                        let host = node.serverID.host.clone();
                                                        let port = node.serverID.port.clone();

                                                        if node.nodeId == cm.get_node_id() {
                                                            <EventBus<CM>>::call_local_func(&inner_consummers, &inner_sender, &mut mut_msg, &address, inner_ev.clone().unwrap())
                                                        } else {
                                                            debug!("{:?}", node);
                                                            let node_id = node.nodeId.clone();
                                                            RUNTIME.spawn(async move {
                                                                let tcp_stream = TCPS.get(&node_id);
                                                                match tcp_stream {
                                                                    Some(stream) => <EventBus<CM>>::get_stream(&mut mut_msg, stream).await,

                                                                    None => {
                                                                        <EventBus<CM>>::create_stream(&mut mut_msg, host, port, node_id).await;
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    }
                                                },
                                                None => {
                                                    let callback = inner_cf.lock().unwrap().remove(&address);
                                                    match callback {
                                                        Some(caller) => {
                                                            caller.call((&mut_msg, inner_ev.clone().unwrap()));
                                                        },
                                                        None => {}
                                                    }
                                                }
                                            }
                                        },
                                        None => {
                                            // NoClusterManager
                                            <EventBus<CM>>::call_local_func(&inner_consummers, &inner_sender, &mut mut_msg, &address, inner_ev.clone().unwrap());
                                        }
                                    }
                                },
                                None => <EventBus<CM>>::call_replay(inner_cf, &mut_msg, inner_ev.clone().unwrap())
                            }
                        });
                    },
                    Err(_err) => {
                        error!("{:?}", _err);
                    }
                }
            }
        });
        self.receiver_joiner = Arc::new(joiner);
    }

    #[inline]
    fn call_replay(inner_cf: Arc<Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>,) + Send + Sync + UnwindSafe>>>>, mut_msg: &Message, ev: Arc<EventBus<CM>>) {
        let address = mut_msg.replay.clone().unwrap();
        let callback = inner_cf.lock().unwrap().remove(&address);
        match callback {
            Some(caller) => {
                caller.call((mut_msg, ev,));
            },
            None => {}
        }
    }

    #[inline]
    fn call_local_func(inner_consummers: &Arc<HashMap<String, Box<dyn Fn(&mut Message,Arc<EventBus<CM>>,) + Send + Sync>>>,
                       inner_sender: &Sender<Message>,
                       mut mut_msg: &mut Message,
                       address: &String,
                        ev: Arc<EventBus<CM>>) {
        let callback = inner_consummers.get(&address.clone());
        match callback {
            Some(caller) => {
                caller.call((mut_msg, ev,));
                inner_sender.send(mut_msg.clone()).unwrap();
            },
            None => {}
        }
    }

    #[inline]
    async fn get_stream(mut_msg: &mut Message, stream: &Arc<TcpStream>) {
        let mut stream = stream.clone();
        unsafe {
            let tcps = Arc::get_mut_unchecked(&mut stream);
            match tcps.write_all(&mut_msg.to_vec().unwrap()).await {
                Ok(_r) => {
                    let mut response = [0u8; 1];
                    let _ = tcps.read(&mut response);
                },
                Err(e) => {
                    warn!("Error in send message: {:?}", e);
                }
            }
        }
    }

    #[inline]
    async fn create_stream(mut_msg: &mut Message, host: String, port: i32, node_id: String) {
        match TcpStream::connect(format!("{}:{}", host, port)).await {
            Ok(mut stream) => {
                match stream.write_all(&mut_msg.to_vec().unwrap()).await {
                    Ok(_r) => {
                        let mut response = [0u8; 1];
                        let _ = stream.read(&mut response);
                        let mut tcps = TCPS.clone();
                        unsafe {
                            let tcps = Arc::get_mut_unchecked(&mut tcps);
                            tcps.insert(node_id, Arc::new(stream));
                        }
                    },
                    Err(e) => {
                        warn!("Error in send message: {:?}", e);
                    }
                }
            },
            Err(err) => {
                warn!("Error in send message: {:?}", err);
            }
        }
    }

    #[inline]
    fn registry_in_cm(&mut self) {
        let opt_cm = Arc::get_mut(&mut self.cluster_manager).unwrap();
        match opt_cm {
            Some(cm) => {
                cm.set_cluster_node_info(ClusterNodeInfo {
                    nodeId: cm.get_node_id(),
                    serverID: ServerID {
                        host: self.options.vertx_host.clone(),
                        port: self.event_bus_port as i32
                    }
                });
            },
            None => {}
        }
    }

    fn create_net_server(&mut self) -> &mut NetServer<CM> {
        let mut net_server = net::NetServer::<CM>::new(self.self_arc.clone());
        net_server.listen_for_message(self.options.vertx_port,  move |req, send| {
            let resp = vec![];
            let msg = Message::from(req);
            trace!("net_server => {:?}", msg);

            let _ = send.send(msg);

            return resp;
        });
        net_server
    }

    pub fn consumer<OP> (&self, address: &str,  op: OP)
        where OP : Fn(&mut Message,Arc<EventBus<CM>>,) + Send + 'static + Sync, {
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
        where OP : Fn(& Message,Arc<EventBus<CM>>,) + Send + 'static + Sync + UnwindSafe, {
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
        let local_sender = self.sender.lock().unwrap();
        local_sender.send(message).unwrap();
    }

}