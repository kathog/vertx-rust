use core::fmt::Debug;
use rayon::prelude::*;
use rayon::{ThreadPoolBuilder, ThreadPool};
use rand::{thread_rng, Rng};
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
        RwLock,
    },
    thread::JoinHandle,
    rc::Rc,
    panic::*,
};
use log::{info, debug};
use waiter_di::*;
use multimap::MultiMap;
use jvm_serializable::java::io::*;
use serde::{Serialize, Deserialize};

#[jvm_object(io.vertx.core.net.impl.ServerID,5636540499169644934)]
pub struct ServerID {
    port: i32,
    host: String
}


#[jvm_object(io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo,1)]
pub struct ClusterNodeInfo {
    pub nodeId: String,
    serverID: ServerID,
}


pub trait ClusterManager {

    fn set_vertx(&mut self, vertx: Arc<Vertx>);

    fn get_node_id(&self) -> String;

    fn get_nodes(&self) -> Vec<String>;

    fn get_ha_infos(&self) -> Arc<Mutex<Vec<ClusterNodeInfo>>>;
    fn get_subs(&self) -> Arc<Mutex<MultiMap<String, ClusterNodeInfo>>>;

    fn join(&mut self);

    fn leave(&self);

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
        }
    }
}

/*
int message_size;
int protocol_version;
int system_codec_id;
bool send;
std::string address;
std::string replay;
int port;
std::string host;
int headers;
std::any _body;
mutable std::any _reply;
bool request;
MsgCallback func;
MsgCallback callbackFunc;
bool local = false;
*/

// #[derive(Clone)]
pub struct Message {
    address: Option<String>,
    callback_address: String,
    body: Arc<Vec<u8>>,
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

impl Debug for Message {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("address", &self.address)
            .field("callback_address", &self.callback_address)
            .field("body", &self.body)
            .finish()
    }
}


pub struct Vertx {
    options : VertxOptions,
    worker_pool: ThreadPool,
    event_bus: Arc<EventBus>,
}

impl Vertx {

    pub fn new (options: VertxOptions) -> Vertx {
        let worker_pool = ThreadPoolBuilder::new().num_threads(options.worker_pool_size).build().unwrap();
        let event_bus = EventBus::new(options.event_bus_options.clone());
        return Vertx {
            options,
            worker_pool,
            event_bus : Arc::new(event_bus),
        };
    }



    pub fn start(&self) {
        self.event_bus.start();
    }

    pub fn event_bus(&self) -> Arc<EventBus> {
        return self.event_bus.clone();
    }

}

pub struct EventBus {
    options : EventBusOptions,
    event_bus_pool: Arc<ThreadPool>,
    consumers: Arc<HashMap<String, Box<dyn Fn(&mut Message) + Send + Sync>>>,
    callback_functions: Arc<Mutex<HashMap<String, Box<dyn Fn(&Message) + Send + Sync + UnwindSafe>>>>,
    sender: Mutex<Sender<Message>>,
    receiver_joiner : Arc<JoinHandle<()>>,
}

impl EventBus {

    pub fn new (options: EventBusOptions) -> EventBus {
        let event_bus_pool = ThreadPoolBuilder::new().num_threads(options.event_bus_pool_size).build().unwrap();
        let (sender, _receiver) : (Sender<Message>, Receiver<Message>) = channel();
        let receiver_joiner = std::thread::spawn(||{});
        let mut ev = EventBus {
            options,
            event_bus_pool : Arc::new(event_bus_pool),
            consumers: Arc::new(HashMap::new()),
            callback_functions: Arc::new(Mutex::new(HashMap::new())),
            sender : Mutex::new(sender),
            receiver_joiner : Arc::new(receiver_joiner),
        };
        ev.init();
        return ev;
    }

    fn start (&self) {
        let joiner = &self.receiver_joiner;
        let h = joiner.clone();
        unsafe {
            let val :JoinHandle<()> = std::ptr::read(&*h);
            val.join().unwrap();
        }
    }

    fn init(&mut self)  {
        let (sender, receiver) : (Sender<Message>, Receiver<Message>) = channel();
        self.sender = Mutex::new(sender);
        let local_consumers = self.consumers.clone();
        let local_cf = self.callback_functions.clone();
        let pool = self.event_bus_pool.clone();
        let local_sender = self.sender.lock().unwrap().clone();
        let joiner = std::thread::spawn(move || -> (){
            loop {
                match receiver.recv() {
                    Ok(msg) => {
                        debug!("{:?}", msg);
                        let inner_consummers = local_consumers.clone();
                        let inner_cf = local_cf.clone();
                        let inner_sender = local_sender.clone();

                        pool.spawn(move || {
                            let mut mut_msg = msg;
                            match &mut_msg.address {
                                Some(address) => {
                                    let callback = inner_consummers.get(address);
                                    match callback {
                                        Some(caller) => {
                                            caller.call((&mut mut_msg,));
                                            mut_msg.address = None;
                                            inner_sender.send(mut_msg).unwrap();
                                        },
                                        None => {}
                                    }
                                },
                                None => {
                                    let address = mut_msg.callback_address.clone();
                                    let callback = inner_cf.lock().unwrap().remove(&address);
                                    match callback {
                                        Some(caller) => {
                                            caller.call((&mut mut_msg,));
                                        },
                                        None => {}
                                    }
                                }
                            }
                        });

                    },
                    Err(_err) => {
                        // println!("{:?}", err);
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
    }

    pub fn request(&self, address: &str, request: String) {
        let addr = address.to_owned();
        let body = request.as_bytes().to_vec();
        let message = Message {
            address: Some(addr.clone()),
            callback_address: uuid::Uuid::new_v4().to_string(),
            body: Arc::new(body),
        };
        let local_sender = self.sender.lock().unwrap().clone();
        local_sender.send(message).unwrap();
    }

    pub fn request_with_callback<OP> (&self, address: &str, request: String, op: OP)
        where OP : Fn(& Message,) + Send + 'static + Sync + UnwindSafe, {
        let addr = address.to_owned();
        let body = request.as_bytes().to_vec();
        let message = Message {
            address: Some(addr.clone()),
            callback_address: format!("__vertx.reply.{}", uuid::Uuid::new_v4().to_string()),
            body: Arc::new(body),
        };
        let local_cons = self.callback_functions.clone();
        local_cons.lock().unwrap().insert(message.callback_address.clone(), Box::new(op));
        let local_sender = self.sender.lock().unwrap().clone();
        local_sender.send(message).unwrap();
    }

}