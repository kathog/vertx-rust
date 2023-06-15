pub mod cm;
pub mod message;

use crate::http::HttpServer;
use crate::net;
use crate::net::NetServer;
use crate::vertx::cm::{ClusterManager, ClusterNodeInfo, ServerID};
use crate::vertx::message::{Message, Body, MessageInner};
use core::fmt::Debug;
use hashbrown::HashMap;
use log::{debug, error, info, trace, warn};
use signal_hook::iterator::Signals;
use std::sync::atomic::{AtomicBool, AtomicI16, Ordering};
use std::{
    sync::{Arc},
};
use std::future::Future;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::process::exit;
use atomic_refcell::AtomicRefCell;
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use futures::FutureExt;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static + UnwindSafe>>;
type PinBoxFnMessage<CM> = Pin<Box<dyn Fn(Arc<Message>, Arc<EventBus<CM>>) -> BoxFuture<()> + Send + 'static + Sync + RefUnwindSafe>>;


pub fn wrap_with_catch_unwind<CM, F>(func: F) -> PinBoxFnMessage<CM>
    where
        CM: ClusterManager + Send + Sync + 'static + RefUnwindSafe,
        F: Fn(Arc<Message>, Arc<EventBus<CM>>) -> BoxFuture<()> + Send + 'static + Sync + RefUnwindSafe,
{
    let wrapped_func = move |msg: Arc<Message>, bus: Arc<EventBus<CM>>| {
        let result = std::panic::catch_unwind(|| func(msg.clone(), bus.clone()));
        match result {
            Ok(future) => future,
            Err(err) => {
                let body = if let Some (err_msg) = err.downcast_ref::<&str>() {
                    error!("{:?} in function: {:?}", err_msg, std::any::type_name::<F>());
                    Body::Panic(format!("{:?} in function: {:?}", err_msg, std::any::type_name::<F>()))
                } else {
                    Body::Panic("Unknown panic!".to_string())
                };
                Box::pin(async move {
                    msg.reply(body);
                })
            }
        }
    };

    Box::pin(wrapped_func)
}

lazy_static! {
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

pub struct Vertx<CM: 'static + ClusterManager + Send + Sync + RefUnwindSafe> {
    #[allow(dead_code)]
    options: VertxOptions,
    event_bus: Arc<EventBus<CM>>,
    ph: PhantomData<CM>,
}

impl<CM: 'static + ClusterManager + Send + Sync + RefUnwindSafe> Vertx<CM> {
    pub fn new(options: VertxOptions) -> Vertx<CM> {
        let event_bus = EventBus::<CM>::new(options.event_bus_options.clone());
        Vertx {
            options,
            event_bus: Arc::new(event_bus),
            ph: PhantomData,
        }
    }

    pub fn set_cluster_manager(&mut self, cm: CM) {
        debug!("set_cluster_manager: {:?}", cm.get_nodes());
        Arc::get_mut(&mut self.event_bus)
            .unwrap()
            .set_cluster_manager(cm);
    }

    pub async fn start(&self) {
        info!("start vertx version {}", env!("CARGO_PKG_VERSION"));
        let mut signals = Signals::new(&[2]).unwrap();
        let event_bus = self.event_bus.clone();
        tokio::spawn(async move {
            let sig = signals.forever().next();
            info!("Stopping vertx with signal: {:?}", sig.unwrap());
            drop(event_bus.sender.data_ptr());
            exit(sig.unwrap());
        });
        self.event_bus.start().await;
    }

    pub async fn create_http_server(&self) -> HttpServer<CM> {
        let _ = self.event_bus().await;
        HttpServer::new(Some(self.event_bus.clone()))
    }

    pub async fn create_net_server(&self) -> &'static mut NetServer<CM> {
        let _ = self.event_bus().await;
        NetServer::new(Some(self.event_bus.clone()))
    }

    pub async fn event_bus(&self) -> Arc<EventBus<CM>> {
        if !self.event_bus.init {
            let mut ev = self.event_bus.clone();
            unsafe {
                let opt_ev = Arc::get_mut_unchecked(&mut ev);
                opt_ev.init().await;
                opt_ev.init = true;
            }
        }
        self.event_bus.clone()
    }

    pub async fn stop(&self) {
        self.event_bus.stop().await;
    }
}

pub struct EventBus<CM: 'static + ClusterManager + Send + Sync+ RefUnwindSafe> {
    options: EventBusOptions,
    consumers: Arc<HashMap<String, PinBoxFnMessage<CM>>>,
    consumers_async: Arc<HashMap<String, PinBoxFnMessage<CM>>>,
    callback_functions:
        Arc<DashMap<String, PinBoxFnMessage<CM>>>,
    pub(crate) sender: Mutex<Sender<Arc<Message>>>,
    receiver_joiner: Arc<JoinHandle<()>>,
    cluster_manager: Arc<Option<CM>>,
    event_bus_port: u16,
    self_arc: Option<Arc<EventBus<CM>>>,
    init: bool,
}

impl <CM: 'static + ClusterManager + Send + Sync + RefUnwindSafe> RefUnwindSafe for EventBus<CM> {}

impl<CM: 'static + ClusterManager + Send + Sync + RefUnwindSafe> EventBus<CM> {
    pub fn new(options: EventBusOptions) -> EventBus<CM> {
        let (sender, _): (Sender<Arc<Message>>, Receiver<Arc<Message>>) = bounded(1);
        let receiver_joiner = tokio::spawn(async {});
        let ev = EventBus {
            options,
            consumers: Arc::new(HashMap::new()),
            consumers_async: Arc::new(HashMap::new()),
            callback_functions: Arc::new(DashMap::new()),
            sender: Mutex::new(sender),
            receiver_joiner: Arc::new(receiver_joiner),
            cluster_manager: Arc::new(None),
            event_bus_port: 0,
            self_arc: None,
            init: false,
        };
        ev
    }

    fn set_cluster_manager(&mut self, cm: CM) {
        let mut m = cm;
        m.join();
        self.cluster_manager = Arc::new(Some(m));
    }

    async fn start(&self) {
        let joiner = &self.receiver_joiner;
        let h = joiner.clone();
        unsafe {
            let val: JoinHandle<()> = std::ptr::read(&*h);
            val.await.unwrap();
        }
    }

    async fn stop(&self) {
        info!("stopping event_bus");
        DO_INVOKE.store(false, Ordering::Relaxed);
        self.send("stop", Body::String("stop".to_string()));
    }

    async fn init(&mut self) {
        let (sender, receiver): (Sender<Arc<Message>>, Receiver<Arc<Message>>) = bounded(self.options.event_bus_queue_size);
        self.sender = Mutex::new(sender);
        let local_consumers = self.consumers.clone();
        let local_cf = self.callback_functions.clone();
        let local_sender = self.sender.lock().clone();
        self.self_arc = Some(unsafe { Arc::from_raw(self) });

        let net_server = self.create_net_server().await;
        self.event_bus_port = net_server.port;
        self.registry_in_cm();
        info!(
            "start event_bus on tcp://{}:{}",
            self.options.vertx_host, self.event_bus_port
        );

        self.prepare_consumer_msg(receiver, local_consumers, local_cf, local_sender).await;
    }

    async fn prepare_consumer_msg(
        &mut self,
        receiver: Receiver<Arc<Message>>,
        local_consumers: Arc<
            HashMap<String, PinBoxFnMessage<CM>>,
        >,
        local_cf: Arc<
            DashMap<String, PinBoxFnMessage<CM>>,
        >,
        local_sender: Sender<Arc<Message>>,
    ) {
        let local_cm = self.cluster_manager.clone();
        let local_ev = self.self_arc.clone();
        let local_local_consumers = self.consumers_async.clone();

        let joiner = tokio::spawn(async move {
            loop {
                if !DO_INVOKE.load(Ordering::Relaxed) {
                    return;
                }
                match receiver.recv() {
                    Ok(msg) => {
                        trace!("{:?} - {:?}", msg.invoke_count, msg.inner.borrow());
                        let inner_consummers = local_consumers.clone();
                        let inner_cf = local_cf.clone();
                        let inner_sender = local_sender.clone();
                        let inner_cm = local_cm.clone();
                        let inner_ev = local_ev.clone();
                        let inner_local_consumers = local_local_consumers.clone();

                        // tokio::spawn(async move {
                            let mut_msg = msg;
                            match mut_msg.address() {
                                Some(address) => {
                                    if inner_local_consumers.contains_key(&address) {
                                        <EventBus<CM>>::call_local_async(
                                            &inner_local_consumers,
                                            &inner_sender,
                                            mut_msg,
                                            &address,
                                            inner_ev.clone().unwrap(),
                                            inner_cf,
                                        ).await;
                                        // return;
                                    } else {
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
                                                let nodes_lock = {
                                                    let subs = cm.get_subs();
                                                    let nodes = subs.read().unwrap();
                                                    match nodes.get_vec(&address) {
                                                        None => None,
                                                        Some(n) => {
                                                            Some(n.to_vec())
                                                        }
                                                    }
                                                };

                                                match nodes_lock {
                                                    Some(n) => {
                                                        if n.is_empty() {
                                                            warn!("subs not found");
                                                        } else {
                                                            <EventBus<CM>>::send_message(
                                                                &inner_consummers,
                                                                &inner_sender,
                                                                &inner_ev,
                                                                mut_msg,
                                                                &address,
                                                                cm,
                                                                &n,
                                                                inner_cf,
                                                            ).await;
                                                        }
                                                    }
                                                    None => {
                                                        let callback = {
                                                            inner_cf.remove(&address)
                                                        };

                                                        match callback {
                                                            Some(caller) => {
                                                                match caller.1(mut_msg.clone(), inner_ev.clone().unwrap()).catch_unwind().await {
                                                                    Ok(_) => {},
                                                                    Err(err) => {
                                                                        if let Some (err_msg) = err.downcast_ref::<&str>() {
                                                                            error!("{:?}", err_msg);
                                                                            mut_msg.reply(Body::Panic(err_msg.to_string()));
                                                                        } else {
                                                                            mut_msg.reply(Body::Panic("Unknown panic!".to_string()));
                                                                        }

                                                                    }
                                                                }
                                                            }
                                                            None => {
                                                                let host = mut_msg.inner.borrow().host.clone();
                                                                let port = mut_msg.inner.borrow().port;
                                                                <EventBus<CM>>::send_reply(mut_msg, host, port).await;
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
                                                    mut_msg,
                                                    &address,
                                                    inner_ev.clone().unwrap(),
                                                    inner_cf,
                                                ).await;
                                            }
                                        }
                                    }

                                }
                                None => <EventBus<CM>>::call_replay(
                                    inner_cf,
                                    mut_msg,
                                    inner_ev.clone().unwrap(),
                                ).await,
                            }
                        // }).await;
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                    }
                }
            }
        });
        self.receiver_joiner = Arc::new(joiner);
    }

    #[inline]
    async fn send_message(
        inner_consummers: &Arc<
            HashMap<String, PinBoxFnMessage<CM>>,
        >,
        inner_sender: &Sender<Arc<Message>>,
        inner_ev: &Option<Arc<EventBus<CM>>>,
        mut_msg: Arc<Message>,
        address: &str,
        cm: &mut CM,
        nodes: &[ClusterNodeInfo],
        inner_cf: Arc<
            DashMap<String, PinBoxFnMessage<CM>>,
        >,
    ) {
        if mut_msg.inner.borrow().publish {
            for node in nodes {
                let mut node = Some(node);
                <EventBus<CM>>::send_to_node(
                    &inner_consummers,
                    &inner_sender,
                    inner_ev,
                    mut_msg.clone(),
                    &address,
                    cm,
                    inner_cf.clone(),
                    &mut node,
                ).await
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
                mut_msg,
                &address,
                cm,
                inner_cf,
                &mut node,
            ).await
        }
    }

    #[inline]
    async fn send_to_node(
        inner_consummers: &&Arc<
            HashMap<String, PinBoxFnMessage<CM>>,
        >,
        inner_sender: &&Sender<Arc<Message>>,
        inner_ev: &Option<Arc<EventBus<CM>>>,
        mut_msg: Arc<Message>,
        address: &&str,
        cm: &mut CM,
        inner_cf: Arc<
            DashMap<String, PinBoxFnMessage<CM>>,
        >,
        node: &mut Option<&ClusterNodeInfo>,
    ) {
        let node = node.unwrap();
        let host = node.serverID.host.clone();
        let port = node.serverID.port;

        if node.nodeId == cm.get_node_id() {
            <EventBus<CM>>::call_local_func(
                &inner_consummers,
                &inner_sender,
                mut_msg,
                &address,
                inner_ev.clone().unwrap(),
                inner_cf,
            ).await
        } else {
            debug!("{:?}", node);
            let node_id = node.nodeId.clone();
            // let mut message: &'static mut Message = Box::leak(Box::from(mut_msg.clone()));
            let message = mut_msg.clone();
            let tcp_stream = TCPS.get(&node_id);
            match tcp_stream {
                Some(stream) => <EventBus<CM>>::get_stream(message, stream).await,

                None => {
                    <EventBus<CM>>::create_stream(message, host, port, node_id).await;
                }
            }
        }
    }

    #[inline]
    async fn call_replay(
        inner_cf: Arc<
            DashMap<String, PinBoxFnMessage<CM>>,
        >,
        mut_msg: Arc<Message>,
        ev: Arc<EventBus<CM>>,
    ) {
        let address = mut_msg.replay();
        if let Some(address) = address {
            // let mut map = inner_cf.lock();
            let callback = inner_cf.remove(&address);
            if let Some(caller) = callback {

                match caller.1(mut_msg.clone(), ev).catch_unwind().await {
                    Ok(_) => {},
                    Err(err) => {
                        if let Some (err_msg) = err.downcast_ref::<&str>() {
                            error!("{:?}", err_msg);
                            mut_msg.reply(Body::Panic(err_msg.to_string()));
                        } else {
                            mut_msg.reply(Body::Panic("Unknown panic!".to_string()));
                        }

                    }
                }
            }
        }
    }

    #[inline]
    async fn call_local_func(
        inner_consummers: &Arc<
            HashMap<String, PinBoxFnMessage<CM>>,
        >,
        inner_sender: &Sender<Arc<Message>>,
        mut_msg: Arc<Message>,
        address: &str,
        ev: Arc<EventBus<CM>>,
        inner_cf: Arc<
            DashMap<String, PinBoxFnMessage<CM>>,
        >,
    ) {
        let callback = inner_consummers.get(&address.to_string());
        match callback {
            Some(caller) => {
                if !mut_msg.invoke.load(Ordering::SeqCst) {
                    match caller(mut_msg.clone(), ev).catch_unwind().await {
                        Ok(_) => {},
                        Err(err) => {
                            if let Some (err_msg) = err.downcast_ref::<&str>() {
                                error!("{:?}", err_msg);
                                mut_msg.reply(Body::Panic(err_msg.to_string()));
                            } else {
                                mut_msg.reply(Body::Panic("Unknown panic!".to_string()));
                            }

                        }
                    }
                    mut_msg.invoke.store(true, Ordering::SeqCst);
                }
                if mut_msg.inner.borrow().address.is_some() {
                    if mut_msg.invoke_count.load(Ordering::SeqCst) == 5 {
                        mut_msg.reply(Body::Panic("Message got stuck in a loop probably due to an error in the reply to the sub-message. Message deleted!".to_string()));
                        inner_sender.send(mut_msg.clone()).unwrap();
                    } else if mut_msg.invoke_count.load(Ordering::SeqCst) < 5  {
                        inner_sender.send(mut_msg.clone()).unwrap();
                    }
                }
            }
            None => {
                let callback = inner_cf.remove(address);
                if let Some(caller) = callback {
                    let msg = mut_msg.clone();
                    match caller.1.call((msg, ev)).catch_unwind().await {
                        Ok(_) => {},
                        Err(err) => {
                            if let Some (err_msg) = err.downcast_ref::<&str>() {
                                error!("{:?}", err_msg);
                                mut_msg.reply(Body::Panic(err_msg.to_string()));
                            } else {
                                mut_msg.reply(Body::Panic("Unknown panic!".to_string()));
                            }

                        }
                    }
                } else { // wysłanie odpowiedzi do requesta
                    let address = mut_msg.inner.borrow().replay.clone();
                    if let Some(address) = address {
                        let callback = inner_cf.remove(&address);
                        if let Some(caller) = callback {
                            match caller.1.call((mut_msg.clone(), ev)).catch_unwind().await {
                                Ok(_) => {},
                                Err(err) => {
                                    if let Some (err_msg) = err.downcast_ref::<&str>() {
                                        error!("{:?}", err_msg);
                                        mut_msg.reply(Body::Panic(err_msg.to_string()));
                                    } else {
                                        mut_msg.reply(Body::Panic("Unknown panic!".to_string()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn call_local_async(
        inner_consummers: &Arc<
            HashMap<String, PinBoxFnMessage<CM>>,
        >,
        inner_sender: &Sender<Arc<Message>>,
        mut_msg: Arc<Message>,
        address: &str,
        ev: Arc<EventBus<CM>>,
        inner_cf: Arc<
            DashMap<String, PinBoxFnMessage<CM>>,
        >,
    ) {
        let callback = inner_consummers.get(&address.to_string());
        match callback {
            Some(caller) => {
                mut_msg.invoke_count.fetch_add(1, Ordering::SeqCst);
                if !mut_msg.invoke.load(Ordering::SeqCst) {
                    match caller(mut_msg.clone(), ev).catch_unwind().await {
                        Ok(_) => {},
                        Err(err) => {
                            if let Some (err_msg) = err.downcast_ref::<&str>() {
                                error!("{:?}", err_msg);
                                mut_msg.reply(Body::Panic(err_msg.to_string()));
                            } else {
                                mut_msg.reply(Body::Panic("Unknown panic!".to_string()));
                            }

                        }
                    }
                    mut_msg.invoke.store(true, Ordering::SeqCst);
                }
                if mut_msg.inner.borrow().address.is_some() {
                    if mut_msg.invoke_count.load(Ordering::SeqCst) == 5 {
                        mut_msg.reply(Body::Panic("Message got stuck in a loop probably due to an error in the reply to the sub-message. Message deleted! ".to_string()));
                        inner_sender.send(mut_msg.clone()).unwrap();
                    } else if mut_msg.invoke_count.load(Ordering::SeqCst) < 5  {
                        inner_sender.send(mut_msg.clone()).unwrap();
                    }
                }
            }
            None => {
                let callback = inner_cf.remove(address);
                if let Some(caller) = callback {
                    match caller.1.call((mut_msg.clone(), ev)).catch_unwind().await {
                        Ok(_) => {},
                        Err(err) => {
                            if let Some (err_msg) = err.downcast_ref::<&str>() {
                                error!("{:?}", err_msg);
                                mut_msg.reply(Body::Panic(err_msg.to_string()));
                            } else {
                                mut_msg.reply(Body::Panic("Unknown panic!".to_string()));
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn get_stream(mut_msg: Arc<Message>, stream: &Arc<TcpStream>) {
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
    async fn create_stream(mut_msg: Arc<Message>, host: String, port: i32, node_id: String) {
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
    async fn send_reply(mut_msg: Arc<Message>, host: String, port: i32) {
        // tokio::spawn(async move {

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
        // });

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

    async fn create_net_server(&mut self) -> &mut NetServer<CM> {
        let net_server = net::NetServer::<CM>::new(self.self_arc.clone());
        net_server.listen_for_message(self.options.vertx_port, move |req, send| {
            let resp = vec![];
            let msg = Arc::new(Message::from(req));
            debug!("net_server => {:?}", msg);

            let _ = send.send(msg);

            resp
        }).await;
        net_server
    }

    pub fn local_consumer<OP>(&self, address: &str, op: OP)
        where
            OP: Fn(Arc<Message>, Arc<EventBus<CM>>) -> BoxFuture<()> + Send + 'static + Sync + RefUnwindSafe,
    {
        let local_op = wrap_with_catch_unwind(op);
        unsafe {
            let mut local_cons = self.consumers_async.clone();
            Arc::get_mut_unchecked(&mut local_cons).insert(address.to_string(), local_op);
        }
    }

    pub fn consumer<OP>(&self, address: &str, op: OP)
    where
        OP: Fn(Arc<Message>, Arc<EventBus<CM>>) -> BoxFuture<()> + Send + 'static + Sync + RefUnwindSafe,
    {
        unsafe {
            let mut local_cons = self.consumers.clone();
            let local_op = wrap_with_catch_unwind(op);
            Arc::get_mut_unchecked(&mut local_cons).insert(address.to_string(), local_op);
        }
        match self.cluster_manager.as_ref() {
            Some(cm) => {
                cm.add_sub(address.to_string());
            }
            None => {}
        };
    }

    #[inline]
    pub fn send(&self, address: &str, request: Body) {
        let addr = address.to_owned();
        let message_inner = MessageInner {
            address: Some(addr),
            replay: None,
            body: Arc::new(request),
            ..Default::default()
        };
        let message = Message{
            inner: AtomicRefCell::new(message_inner),
            invoke: Default::default(),
            invoke_count: AtomicI16::new(0)
        };
        let local_sender = self.sender.lock();
        local_sender.send(Arc::new(message)).unwrap();
    }

    #[inline]
    pub fn publish(&self, address: &str, request: Body) {
        let addr = address.to_owned();
        let message_inner = MessageInner {
            address: Some(addr),
            replay: None,
            body: Arc::new(request),
            publish: true,
            ..Default::default()
        };
        let message = Message{
            inner: AtomicRefCell::new(message_inner),
            invoke: Default::default(),
            invoke_count: AtomicI16::new(0)
        };
        let local_sender = self.sender.lock();
        local_sender.send(Arc::new(message)).unwrap();
    }

    #[inline]
    pub fn request<OP>(&self, address: &str, request: Body, op: OP)
    where
        OP: Fn(Arc<Message>, Arc<EventBus<CM>>) -> BoxFuture<()> + Send + 'static + Sync + RefUnwindSafe,
    {
        let addr = address.to_owned();
        let message_inner = MessageInner {
            address: Some(addr),
            replay: Some(format!(
                "__vertx.reply.{}",
                uuid::Uuid::new_v4().to_string()
            )),
            body: Arc::new(request),
            host: self.options.vertx_host.clone(),
            port: self.event_bus_port as i32,
            ..Default::default()
        };
        let message = Message{
            inner: AtomicRefCell::new(message_inner),
            invoke: Default::default(),
            invoke_count: AtomicI16::new(0)
        };
        let local_cons = self.callback_functions.clone();
        let local_op = wrap_with_catch_unwind(op);
        local_cons
            .insert(message.replay().unwrap(), local_op);
        let local_sender = self.sender.lock();
        if let Err(err) = local_sender.send(Arc::new(message)) {
            warn!("{:?}", err);
        }
    }
}
