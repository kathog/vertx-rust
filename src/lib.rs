#![feature(get_mut_unchecked)]
#![feature(fn_traits)]
#![feature(plugin)]
#[allow(non_upper_case_globals)]
// #![plugin(hypospray_extensions)]

extern crate hypospray;
#[macro_use]
extern crate lazy_static;

#[cfg(test)]
mod tests {

    use crate::vertx;
    use vertx::*;
    use std::sync::Arc;

    use pyo3::{prelude::*, types::*};

    
    #[test]
    #[ignore = "hz off"]
    fn python_test () {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let local_hazelcast = PyModule::from_code(py, r#"
import hazelcast

client = hazelcast.HazelcastClient()

def get_haInfo():
    for key, value in client.get_map("__vertx.haInfo").blocking().entry_set():
        dictionary = {};
        dictionary[key] = value
        return dictionary

def put_haInfo(key, value):
    print("put_haInfo_key: " + key)
    print("put_haInfo_value: " + value)
    haInfo = client.get_map("__vertx.haInfo")
    return haInfo.put(key, value).result()

def shutdown():
    client.shutdown()

        "#, "hazelcast_client.py", "hazelcast_client").unwrap();

        let sufix_value = r#"{"verticles":[],"group":"__DISABLED__","server_id":{"host":""#;
        let value =  format!("{}{}\",\"port\":{}}}", sufix_value, "localhost", 1235);
        println!("{:?}",local_hazelcast.call1("put_haInfo", (uuid::Uuid::new_v4().to_string(), value,)).unwrap());


        let hz = local_hazelcast.call0("get_haInfo").unwrap();
        let hz_list = hz.cast_as::<PyDict>().unwrap();

        for (key, value) in hz_list {
            println!("key: {:?}", key.cast_as::<PyString>().unwrap());
            println!("value: {:?}", value.cast_as::<PyString>().unwrap());
        }

        // std::thread::sleep(std::time::Duration::from_secs(2));

        local_hazelcast.call0("shutdown").unwrap();
    }

    use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper};
    use zookeeper::recipes::cache::*;
    use serde_json::*;
    use serde::{Serialize, Deserialize};
    struct LoggingWatcher;
    impl Watcher for LoggingWatcher {
        fn handle(&self, e: WatchedEvent) {
            println!("{:?}", e);
        }
    }


    use tokio::net::TcpListener;
    use tokio::prelude::*;

    #[test]
    fn tcp_test () {     
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime.block_on(TcpListener::bind("0.0.0.0:9091")).unwrap();
        println!("{:?}", listener);

        loop {
            let (mut socket, _) = runtime.block_on(listener.accept()).unwrap();

            runtime.spawn(async move {
            
                let mut request: Vec<u8> = vec![];
                let mut buf = [0; 1024];

                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(n) if n == 0 => return,
                        Ok(n) => request.extend(&buf[0..n]),
                        Err(e) => {
                            eprintln!("failed to read from socket; err = {:?}", e);
                            return;
                        }
                    };

                    // println!("{:?}", std::str::from_utf8(&request));

                    let data = r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: 14

{"code": "UP"}
"#.to_string();
                    if let Err(e) = socket.write_all(data.as_bytes()).await {
                        eprintln!("failed to write to socket; err = {:?}", e);
                        return;
                    }
                }
            });
        }
    }

    #[test]
    fn zk_test () {  
        use std::time::Duration;
        static ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH : &str = "/cluster/nodes";
        static ZK_PATH_HA_INFO : &str = "/syncMap/__vertx.haInfo";
        static ZK_PATH_SUBS : &str = "/asyncMultiMap/__vertx.subs";
        static ZK_ROOT_NODE : &str = "io.vertx.01";
        static serialize_data : &str = "\u{0}��\u{0}\u{5}sr\u{0}6io.vertx.spi.cluster.zookeeper.impl.ZKSyncMap$KeyValueZ�\u{1a}\u{0}IiLz\u{2}\u{0}\u{2}L\u{0}\u{3}keyt\u{0}\u{12}Ljava/lang/Object;L\u{0}\u{5}valueq\u{0}~\u{0}\u{1}xpt\u{0}$";

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct ServerId {
            host: String,
            port: usize
        }
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct NodeId {
            verticles: Vec<String>,
            group: String,
            server_id: ServerId
        }



        let zk = ZooKeeper::connect(&format!("{}/{}", "127.0.0.1:2181", ZK_ROOT_NODE), Duration::from_secs(15), LoggingWatcher).unwrap();
        zk.add_listener(|zk_state| println!("New ZkState is {:?}", zk_state));
        let zk_arc = Arc::new(zk);
        let mut pcc = PathChildrenCache::new(zk_arc.clone(), ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH).unwrap();
        match pcc.start() {
            Err(err) => {
                println!("error starting cache: {:?}", err);
                return;
            }
            _ => {
                println!("cache started");
            }
        }

        let zk_ark0 = zk_arc.clone();
        pcc.add_listener(move |event| {
            match event {
                PathChildrenCacheEvent::ChildAdded(_id, name) => {
                    println!("ChildAdded id: {:?}, name: {:?}", _id.replace("/cluster/nodes/", ""), std::str::from_utf8(&name.0));
                    match std::str::from_utf8(&name.0) {
                        Ok(node_id) => {
                            match zk_ark0.get_data_w(&format!("{}/{}", ZK_PATH_HA_INFO, node_id), LoggingWatcher) {
                                Ok(data) => {
                                    let node_data = String::from_utf8_lossy(&data.0).replace(serialize_data, "");
                                    let node_split : Vec<&str> = node_data.split("\u{0}U").collect();
                                    let value : NodeId = serde_json::from_str(node_split[1]).unwrap();
                                    println!("node id: {:?}, value: {:?}", node_id, value);
                                    println!("value as string: {}", serde_json::to_string(&value).unwrap());
                                },
                                Err(_) => {}
                            }

                            match zk_ark0.get_children_w(&format!("{}", ZK_PATH_SUBS), LoggingWatcher) {
                                Ok(data ) => {
                                    println!("event.buses: {:?}", data);
                                    for event_bus in data {
                                        match zk_ark0.get_data_w(&format!("{}/{}/{}", ZK_PATH_SUBS, event_bus, node_id), LoggingWatcher) {
                                            Ok(data) => {
                                                println!("__vertx.subs: {:?}", String::from_utf8_lossy(&data.0));
                                            },
                                            Err(_) => {}
                                        }
                                    }
                                },
                                Err(_) => {}
                            }

                            
                        },
                        Err(_) => {}
                    }
                },
                PathChildrenCacheEvent::ChildRemoved(id) => {
                    println!("ChildRemoved: {:?}", id.replace("/cluster/nodes/", ""));
                },
                _ => {}
            }
        });

        // let nodes_result = zk_arc.get_children(ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH, false);
        // match nodes_result {
        //     Ok(nodes) => {
        //         for node in nodes {
        //             let data =  zk_arc.get_data(&format!("{}/{}", ZK_PATH_HA_INFO, node), false).unwrap();
        //             let node_data = String::from_utf8_lossy(&data.0).replace(serialize_data, "");
        //             let node_split : Vec<&str> = node_data.split("\u{0}U").collect();
        //             println!("Node id: {:?}, value: {:?}", node_split[0], node_split[1]);
        //         }
        //     },
        //     Err(e) => {
        //         println!("{:?}", e);
        //     }
        // };


        std::thread::park();

    }

    #[test]
    // #[ignore = "tymczasowo"]
    fn vertx_test() {

        lazy_static! {
            static ref vertx : Vertx = {
                let vertx_options = VertxOptions::default();
                println!("{:?}", vertx_options);
                Vertx::new(vertx_options)
            };
            static ref event_bus : Arc<EventBus> = vertx.event_bus();

            static ref count : std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0); 
        }

        event_bus.consumer("consume1", |m| {
            let body = m.body();
            // println!("{:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());
            m.reply(format!("response => {}", std::str::from_utf8(&body).unwrap()).as_bytes().to_vec());
        });

        let time = std::time::Instant::now();         
        for i in 0..100000 {
            // event_bus.request("consume1", format!("regest: {}", i));
            // count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            event_bus.request_with_callback("consume1", format!("regest: {}", i), move |m| {
                let body = m.body();
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // println!("set_callback_function {:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());
            });
        }
        
        // vertx.start();
        // std::thread::sleep(std::time::Duration::from_millis(1));
        let elapsed = time.elapsed();
        println!("count {:?}, time: {:?}", count.load(std::sync::atomic::Ordering::SeqCst), &elapsed);
        println!("avg time: {:?} ns", (&elapsed.as_nanos() / count.load(std::sync::atomic::Ordering::SeqCst) as u128));
    }
}


pub mod vertx {

    use core::fmt::Debug;
    use rayon::prelude::*;
    use rayon::{ThreadPoolBuilder, ThreadPool};
    use rand::{thread_rng, Rng};
    use std::collections::HashMap;
    // use hashbrown::HashMap;
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
            let mut rng = thread_rng();
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
            let mut rng = thread_rng();
            let vertx_port: u16 = rng.gen_range(32000, 48000);
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

}
