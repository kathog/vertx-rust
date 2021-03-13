pub mod cm;
pub mod message;

use crate::http::HttpServer;
use crate::net;
use crate::net::NetServer;
use crate::vertx::cm::{ClusterManager, ClusterNodeInfo, ServerID};
use crate::vertx::message::{Message, Body};
use core::fmt::Debug;
use crossbeam_channel::*;
use hashbrown::HashMap;
use log::{debug, error, info, trace, warn};
use signal_hook::iterator::Signals;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::marker::PhantomData;

static EV_INIT: Once = Once::new();

lazy_static! {
    pub static ref RUNTIME: Runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    static ref TCPS: Arc<HashMap<String, Arc<TcpStream>>> = Arc::new(HashMap::new());
    static ref DO_INVOKE: AtomicBool = AtomicBool::new(true);
}

//Vertx options
#[derive(Debug, Clone)]
pub struct VertxOptions {
    //Worker pool size, default number of cpu/2
    worker_pool_size: usize,
    //Event bus options
    event_bus_options: EventBusOptions,
}

impl VertxOptions {
    pub fn worker_pool_size(&mut self, size: usize) -> &mut Self {
        self.worker_pool_size = size;
        self
    }

    pub fn event_bus_options(&mut self) -> &mut EventBusOptions {
        &mut self.event_bus_options
    }
}

impl Default for VertxOptions {
    fn default() -> Self {
        let cpus = num_cpus::get();
        let vertx_port: u16 = 0;
        let vertx_host = "127.0.0.1".to_owned();
        VertxOptions {
            worker_pool_size: cpus / 2,
            event_bus_options: EventBusOptions::from((vertx_host, vertx_port)),
        }
    }
}

//Event bus options
#[derive(Debug, Clone)]
pub struct EventBusOptions {
    //Event bus pool size, default number of cpu
    event_bus_pool_size: usize,
    //Event bus queue size, default is 2000
    event_bus_queue_size: usize,
    //Event bus host, default 127.0.0.1
    vertx_host: String,
    //Event bus port, default 0
    vertx_port: u16,
}

impl EventBusOptions {
    pub fn event_bus_pool_size(&mut self, size: usize) -> &mut Self {
        self.event_bus_pool_size = size;
        self
    }

    pub fn host(&mut self, host: String) -> &mut Self {
        self.vertx_host = host;
        self
    }

    pub fn port(&mut self, port: u16) -> &mut Self {
        self.vertx_port = port;
        self
    }

    pub fn event_bus_queue_size(&mut self, size: usize) -> &mut Self {
        self.event_bus_queue_size = size;
        self
    }
}

impl From<(String, u16)> for EventBusOptions {
    fn from(opts: (String, u16)) -> Self {
        let cpus = num_cpus::get();
        EventBusOptions {
            event_bus_pool_size: cpus,
            vertx_host: opts.0,
            vertx_port: opts.1,
            event_bus_queue_size: 2000,
        }
    }
}

impl Default for EventBusOptions {
    fn default() -> Self {
        let cpus = num_cpus::get();
        let vertx_port: u16 = 0;
        EventBusOptions {
            event_bus_pool_size: cpus / 2,
            vertx_host: String::from("127.0.0.1"),
            vertx_port,
            event_bus_queue_size: 2000,
        }
    }
}

pub struct Vertx<CM: 'static + ClusterManager + Send + Sync> {
    #[allow(dead_code)]
    options: VertxOptions,
    event_bus: Arc<EventBus<CM>>,
    ph: PhantomData<CM>,
}

impl<CM: 'static + ClusterManager + Send + Sync> Vertx<CM> {
    pub fn new(options: VertxOptions) -> Vertx<CM> {
        let event_bus = EventBus::<CM>::new(options.event_bus_options.clone());
        return Vertx {
            options,
            event_bus: Arc::new(event_bus),
            ph: PhantomData,
        };
    }

    pub fn set_cluster_manager(&mut self, cm: CM) {
        debug!("set_cluster_manager: {:?}", cm.get_nodes());
        Arc::get_mut(&mut self.event_bus)
            .unwrap()
            .set_cluster_manager(cm);
    }

    pub fn start(&self) {
        info!("start vertx version {}", env!("CARGO_PKG_VERSION"));
        let mut signals = Signals::new(&[2]).unwrap();
        let event_bus = self.event_bus.clone();
        std::thread::spawn(move || {
            for sig in signals.forever() {
                info!("received signal {:?}", sig);
                event_bus.stop();
                break;
            }
        });
        self.event_bus.start();
    }

    pub fn create_http_server(&self) -> HttpServer<CM> {
        HttpServer::new(Some(self.event_bus.clone()))
    }

    pub fn create_net_server(&self) -> &'static mut NetServer<CM> {
        NetServer::new(Some(self.event_bus.clone()))
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

    pub fn stop(&self) {
        self.event_bus.stop();
    }
}

pub struct EventBus<CM: 'static + ClusterManager + Send + Sync> {
    options: EventBusOptions,
    consumers: Arc<HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>) + Send + Sync>>>,
    local_consumers:
        Arc<HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>) + Send + Sync>>>,
    callback_functions:
        Arc<Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>) + Send + Sync>>>>,
    pub(crate) sender: Mutex<Sender<Message>>,
    receiver_joiner: Arc<JoinHandle<()>>,
    cluster_manager: Arc<Option<CM>>,
    event_bus_port: u16,
    self_arc: Option<Arc<EventBus<CM>>>,
    runtime: Arc<Runtime>,
}

impl<CM: 'static + ClusterManager + Send + Sync> EventBus<CM> {
    pub fn new(options: EventBusOptions) -> EventBus<CM> {
        let (sender, _receiver): (Sender<Message>, Receiver<Message>) = unbounded();
        let receiver_joiner = std::thread::spawn(|| {});
        let pool_size = options.event_bus_pool_size;
        let ev = EventBus {
            options,
            consumers: Arc::new(HashMap::new()),
            local_consumers: Arc::new(HashMap::new()),
            callback_functions: Arc::new(Mutex::new(HashMap::new())),
            sender: Mutex::new(sender),
            receiver_joiner: Arc::new(receiver_joiner),
            cluster_manager: Arc::new(None),
            event_bus_port: 0,
            self_arc: None,
            runtime: Arc::new(
                Builder::new_multi_thread()
                    .worker_threads(pool_size)
                    .enable_all()
                    .build()
                    .unwrap(),
            ),
        };
        return ev;
    }

    fn set_cluster_manager(&mut self, cm: CM) {
        let mut m = cm;
        m.join();
        self.cluster_manager = Arc::new(Some(m));
    }

    fn start(&self) {
        let joiner = &self.receiver_joiner;
        let h = joiner.clone();
        unsafe {
            let val: JoinHandle<()> = std::ptr::read(&*h);
            val.join().unwrap();
        }
    }

    fn stop(&self) {
        info!("stopping event_bus");
        DO_INVOKE.store(false, Ordering::Relaxed);
        self.send("stop", Body::String("stop".to_string()));
    }

    fn init(&mut self) {
        let (sender, receiver): (Sender<Message>, Receiver<Message>) =
            bounded(self.options.event_bus_queue_size);
        self.sender = Mutex::new(sender);
        let local_consumers = self.consumers.clone();
        let local_cf = self.callback_functions.clone();
        let local_sender = self.sender.lock().unwrap().clone();
        self.self_arc = Some(unsafe { Arc::from_raw(self) });

        let net_server = self.create_net_server();
        self.event_bus_port = net_server.port;
        self.registry_in_cm();
        info!(
            "start event_bus on tcp://{}:{}",
            self.options.vertx_host, self.event_bus_port
        );

        self.prepare_consumer_msg(receiver, local_consumers, local_cf, local_sender);
    }

    fn prepare_consumer_msg(
        &mut self,
        receiver: Receiver<Message>,
        local_consumers: Arc<
            HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>) + Send + Sync>>,
        >,
        local_cf: Arc<
            Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>) + Send + Sync>>>,
        >,
        local_sender: Sender<Message>,
    ) {
        let local_cm = self.cluster_manager.clone();
        let local_ev = self.self_arc.clone();
        let local_local_consumers = self.local_consumers.clone();
        let runtime = self.runtime.clone();

        let joiner = std::thread::spawn(move || -> () {
            loop {
                if !DO_INVOKE.load(Ordering::Relaxed) {
                    return;
                }
                match receiver.recv() {
                    Ok(msg) => {
                        trace!("{:?}", msg);
                        let inner_consummers = local_consumers.clone();
                        let inner_cf = local_cf.clone();
                        let inner_sender = local_sender.clone();
                        let inner_cm = local_cm.clone();
                        let inner_ev = local_ev.clone();
                        let inner_local_consumers = local_local_consumers.clone();

                        runtime.spawn(async move {
                            let mut mut_msg = msg;
                            match mut_msg.address.clone() {
                                Some(address) => {
                                    if inner_local_consumers.contains_key(&address) {
                                        <EventBus<CM>>::call_local_func(
                                            &inner_local_consumers,
                                            &inner_sender,
                                            &mut mut_msg,
                                            &address,
                                            inner_ev.clone().unwrap(),
                                            inner_cf,
                                        );
                                        return;
                                    }
                                    // invoke function from consumer
                                    let mut inner_cm0 = inner_cm.clone();
                                    let manager = unsafe { Arc::get_mut_unchecked(&mut inner_cm0) };
                                    match manager {
                                        // ClusterManager
                                        Some(cm) => {
                                            debug!(
                                                "manager: {:?}",
                                                cm.get_subs().read().unwrap().len()
                                            );
                                            let subs = cm.get_subs();
                                            let nodes = subs.read().unwrap();
                                            let nodes_lock = nodes.get_vec(&address.clone());

                                            match nodes_lock {
                                                Some(n) => {
                                                    if n.len() == 0 {
                                                        warn!("subs not found");
                                                    } else {
                                                        <EventBus<CM>>::send_message(
                                                            &inner_consummers,
                                                            &inner_sender,
                                                            &inner_ev,
                                                            &mut mut_msg,
                                                            &address,
                                                            cm,
                                                            n,
                                                            inner_cf,
                                                        )
                                                    }
                                                }
                                                None => {
                                                    let mut map = inner_cf.lock().unwrap();
                                                    let callback = map.remove(&address);
                                                    match callback {
                                                        Some(caller) => {
                                                            caller.call((
                                                                &mut_msg,
                                                                inner_ev.clone().unwrap(),
                                                            ));
                                                        }
                                                        None => {
                                                            let host = mut_msg.host.clone();
                                                            let port = mut_msg.port.clone();
                                                            <EventBus<CM>>::send_reply(mut_msg, host, port);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        None => {
                                            // NoClusterManager
                                            <EventBus<CM>>::call_local_func(
                                                &inner_consummers,
                                                &inner_sender,
                                                &mut mut_msg,
                                                &address,
                                                inner_ev.clone().unwrap(),
                                                inner_cf,
                                            );
                                        }
                                    }
                                }
                                None => <EventBus<CM>>::call_replay(
                                    inner_cf,
                                    &mut_msg,
                                    inner_ev.clone().unwrap(),
                                ),
                            }
                        });
                    }
                    Err(_err) => {
                        error!("{:?}", _err);
                    }
                }
            }
        });
        self.receiver_joiner = Arc::new(joiner);
    }

    #[inline]
    fn send_message(
        inner_consummers: &Arc<
            HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>) + Send + Sync>>,
        >,
        inner_sender: &Sender<Message>,
        inner_ev: &Option<Arc<EventBus<CM>>>,
        mut mut_msg: &mut Message,
        address: &String,
        cm: &mut CM,
        nodes: &Vec<ClusterNodeInfo>,
        inner_cf: Arc<
            Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>) + Send + Sync>>>,
        >,
    ) {
        if mut_msg.publish {
            for node in nodes {
                let mut node = Some(node);
                <EventBus<CM>>::send_to_node(
                    &inner_consummers,
                    &inner_sender,
                    inner_ev,
                    &mut mut_msg,
                    &address,
                    cm,
                    inner_cf.clone(),
                    &mut node,
                )
            }
        } else {
            let idx = cm.next(nodes.len());
            let mut node = nodes.get(idx);
            match node {
                Some(_) => {}
                None => {
                    let idx = cm.next(nodes.len());
                    node = nodes.get(idx);
                }
            }
            <EventBus<CM>>::send_to_node(
                &inner_consummers,
                &inner_sender,
                inner_ev,
                &mut mut_msg,
                &address,
                cm,
                inner_cf,
                &mut node,
            )
        }
    }

    #[inline]
    fn send_to_node(
        inner_consummers: &&Arc<
            HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>) + Send + Sync>>,
        >,
        inner_sender: &&Sender<Message>,
        inner_ev: &Option<Arc<EventBus<CM>>>,
        mut mut_msg: &mut &mut Message,
        address: &&String,
        cm: &mut CM,
        inner_cf: Arc<
            Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>) + Send + Sync>>>,
        >,
        node: &mut Option<&ClusterNodeInfo>,
    ) {
        let node = node.unwrap();
        let host = node.serverID.host.clone();
        let port = node.serverID.port.clone();

        if node.nodeId == cm.get_node_id() {
            <EventBus<CM>>::call_local_func(
                &inner_consummers,
                &inner_sender,
                &mut mut_msg,
                &address,
                inner_ev.clone().unwrap(),
                inner_cf,
            )
        } else {
            debug!("{:?}", node);
            let node_id = node.nodeId.clone();
            let mut message: &'static mut Message = Box::leak(Box::from(mut_msg.clone()));
            RUNTIME.spawn(async move {
                let tcp_stream = TCPS.get(&node_id);
                match tcp_stream {
                    Some(stream) => <EventBus<CM>>::get_stream(&mut message, stream).await,

                    None => {
                        <EventBus<CM>>::create_stream(&mut message, host, port, node_id).await;
                    }
                }
            });
        }
    }

    #[inline]
    fn call_replay(
        inner_cf: Arc<
            Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>) + Send + Sync>>>,
        >,
        mut_msg: &Message,
        ev: Arc<EventBus<CM>>,
    ) {
        let address = mut_msg.replay.clone();
        match address {
            Some(address) => {
                let mut map = inner_cf.lock().unwrap();
                let callback = map.remove(&address);
                match callback {
                    Some(caller) => {
                        caller.call((mut_msg, ev.clone()));
                    }
                    None => {}
                }
            }
            None => {}
        }
    }

    #[inline]
    fn call_local_func(
        inner_consummers: &Arc<
            HashMap<String, Box<dyn Fn(&mut Message, Arc<EventBus<CM>>) + Send + Sync>>,
        >,
        inner_sender: &Sender<Message>,
        mut_msg: &mut Message,
        address: &String,
        ev: Arc<EventBus<CM>>,
        inner_cf: Arc<
            Mutex<HashMap<String, Box<dyn Fn(&Message, Arc<EventBus<CM>>) + Send + Sync>>>,
        >,
    ) {
        let callback = inner_consummers.get(&address.clone());
        match callback {
            Some(caller) => {
                caller.call((mut_msg, ev));
                inner_sender.send(mut_msg.clone()).unwrap();
            }
            None => {
                let mut map = inner_cf.lock().unwrap();
                let callback = map.remove(address);
                match callback {
                    Some(caller) => {
                        let msg = mut_msg.clone();
                        caller.call((&msg, ev.clone()));
                    }
                    None => {}
                }
            }
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
                }
                Err(e) => {
                    warn!("Error in send message: {:?}", e);
                }
            }
        }
    }

    #[inline]
    async fn create_stream(mut_msg: &mut Message, host: String, port: i32, node_id: String) {
        match TcpStream::connect(format!("{}:{}", host, port)).await {
            Ok(mut stream) => match stream.write_all(&mut_msg.to_vec().unwrap()).await {
                Ok(_r) => {
                    let mut response = [0u8; 1];
                    let _ = stream.read(&mut response);
                    let mut tcps = TCPS.clone();
                    unsafe {
                        let tcps = Arc::get_mut_unchecked(&mut tcps);
                        tcps.insert(node_id, Arc::new(stream));
                    }
                }
                Err(e) => {
                    warn!("Error in send message: {:?}", e);
                }
            },
            Err(err) => {
                warn!("Error in send message: {:?}", err);
            }
        }
    }

    #[inline]
    fn send_reply(mut_msg: Message, host: String, port: i32) {
        RUNTIME.spawn(async move {

            let tcp_stream = TCPS.get(&format!("{}_{}", host.clone(), port));
            match tcp_stream {
                Some(stream) => unsafe {
                    let mut stream = stream.clone();
                    let stream = Arc::get_mut_unchecked(&mut stream);
                    match stream.write_all(&mut_msg.to_vec().unwrap()).await {
                        Ok(_) => {},
                        Err(e) => {
                            warn!("Error in send message: {:?}", e);
                        }
                    }
                },
                None => {
                    match TcpStream::connect(format!("{}:{}", host, port)).await {
                        Ok(mut stream) => match stream.write_all(&mut_msg.to_vec().unwrap()).await {
                            Ok(_r) => {
                                let mut tcps = TCPS.clone();
                                unsafe {
                                    let tcps = Arc::get_mut_unchecked(&mut tcps);
                                    tcps.insert(format!("{}_{}", host.clone(), port), Arc::new(stream));
                                }
                            }
                            Err(e) => {
                                warn!("Error in send message: {:?}", e);
                            }
                        },
                        Err(err) => {
                            warn!("Error in send message: {:?}", err);
                        }
                    }
                }
            }
        });

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
                        port: self.event_bus_port as i32,
                    },
                });
            }
            None => {}
        }
    }

    fn create_net_server(&mut self) -> &mut NetServer<CM> {
        let net_server = net::NetServer::<CM>::new(self.self_arc.clone());
        net_server.listen_for_message(self.options.vertx_port, move |req, send| {
            let resp = vec![];
            let msg = Message::from(req);
            trace!("net_server => {:?}", msg);

            let _ = send.send(msg);

            return resp;
        });
        net_server
    }

    pub fn consumer<OP>(&self, address: &str, op: OP)
    where
        OP: Fn(&mut Message, Arc<EventBus<CM>>) + Send + 'static + Sync,
    {
        unsafe {
            let mut local_cons = self.consumers.clone();
            Arc::get_mut_unchecked(&mut local_cons).insert(address.to_string(), Box::new(op));
        }
        match self.cluster_manager.as_ref() {
            Some(cm) => {
                cm.add_sub(address.to_string());
            }
            None => {}
        };
    }

    pub fn local_consumer<OP>(&self, address: &str, op: OP)
    where
        OP: Fn(&mut Message, Arc<EventBus<CM>>) + Send + 'static + Sync,
    {
        unsafe {
            let mut local_cons = self.local_consumers.clone();
            Arc::get_mut_unchecked(&mut local_cons).insert(address.to_string(), Box::new(op));
        }
    }

    #[inline]
    pub fn send(&self, address: &str, request: Body) {
        let addr = address.to_owned();
        let message = Message {
            address: Some(addr.clone()),
            replay: None,
            body: Arc::new(request),
            ..Default::default()
        };
        let local_sender = self.sender.lock().unwrap().clone();
        local_sender.send(message).unwrap();
    }

    #[inline]
    pub fn publish(&self, address: &str, request: Body) {
        let addr = address.to_owned();
        let message = Message {
            address: Some(addr.clone()),
            replay: None,
            body: Arc::new(request),
            publish: true,
            ..Default::default()
        };
        let local_sender = self.sender.lock().unwrap().clone();
        local_sender.send(message).unwrap();
    }

    #[inline]
    pub fn request<OP>(&self, address: &str, request: Body, op: OP)
    where
        OP: Fn(&Message, Arc<EventBus<CM>>) + Send + 'static + Sync,
    {
        let addr = address.to_owned();
        let message = Message {
            address: Some(addr.clone()),
            replay: Some(format!(
                "__vertx.reply.{}",
                uuid::Uuid::new_v4().to_string()
            )),
            body: Arc::new(request),
            host: self.options.vertx_host.clone(),
            port: self.event_bus_port as i32,
            ..Default::default()
        };
        let local_cons = self.callback_functions.clone();
        local_cons
            .lock()
            .unwrap()
            .insert(message.replay.clone().unwrap(), Box::new(op));
        let local_sender = self.sender.lock().unwrap();
        local_sender.send(message).unwrap();
    }
}
