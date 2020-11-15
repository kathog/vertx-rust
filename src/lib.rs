#![feature(get_mut_unchecked)]
#![feature(fn_traits)]
#![feature(plugin)]
#[allow(non_upper_case_globals)]
// #![plugin(hypospray_extensions)]

extern crate hypospray;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate jvm_macro;
extern crate jvm_serializable;

#[cfg(test)]
mod tests {

    use crate::vertx;
    use vertx::*;
    use std::sync::Arc;

    // use pyo3::{prelude::*, types::*};

    use j4rs::{Instance, InvocationArg, Jvm, JvmBuilder, ClasspathEntry, MavenArtifact, MavenArtifactRepo, MavenSettings};
    use std::convert::TryFrom;
    use rustc_serialize::base64::FromBase64;
    use core::convert::TryInto;
    use serde_json::*;


    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ServerId {
        host: String,
        port: usize
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct NodeInfo {
        verticles: Vec<String>,
        group: String,
        server_id: ServerId
    }
    
    fn vertx_ha_info_as_node(data : Vec<u8>, jvm : &Jvm) -> (String, NodeInfo) {
        let data_as_i8 : Vec<InvocationArg> = data.into_iter().map(|x| InvocationArg::try_from( x as i8).unwrap()).collect();
        let data_as_java_array = jvm.create_java_array("java.lang.Byte", &data_as_i8).unwrap();
        let to_key_value = jvm.invoke_static("eu.craftsoft.NodeInfoSerializer", "toKeyValue", &vec![InvocationArg::from(data_as_java_array)]);
        let result_as_instance = to_key_value.unwrap();
        let result_as_vector = jvm.to_rust::<Vec<String>>(result_as_instance).unwrap();

        return (result_as_vector[0].clone(), serde_json::from_str(&result_as_vector[1]).unwrap());
    }

    fn vertx_ha_info_as_bytes(id : String, node: NodeInfo, jvm : &Jvm) -> Vec<u8> {
        let id_as_java = InvocationArg::try_from(id).unwrap();
        let node_as_java = InvocationArg::try_from(serde_json::to_string(&node).unwrap()).unwrap();

        let from_key_value = jvm.invoke_static("eu.craftsoft.NodeInfoSerializer", "fromKeyValue", &vec![id_as_java, node_as_java]);
        let result = from_key_value.unwrap();
        let result_as_rust = jvm.to_rust::<String>(result).unwrap();
        return result_as_rust.from_base64().unwrap();
    }



    #[test]
    fn serialize_test () { 
        let time = std::time::Instant::now();
        let entry = ClasspathEntry::new("/home/nerull/dev/repos/java/node-info-serializer/target/node-info-serializer-1.0.0-fat.jar");
        let jvm: Jvm = JvmBuilder::new().classpath_entry(entry).build().unwrap();
        println!("init jvm time: {:?}", time.elapsed());

        for i in 0..1 {
            let time = std::time::Instant::now();

            let node_info = NodeInfo {
                verticles : vec![],
                group: "__DISABLED__".to_owned(),
                server_id : ServerId {
                    host: "localhost".to_owned(),
                    port: 45937
                }
            };

            // let subs_as_bytes = vertx_subs_as_bytes(node, &jvm);
            // // println!("{:?}", String::from_utf8_lossy(&subs_as_bytes));
            // println!("{:?}",  vertx_subs_as_node(subs_as_bytes, &jvm));



            // let v= vertx_ha_info_as_bytes("21a8a87e-55c6-4341-a3b2-67567c9cacfc".to_owned(), node_info, &jvm);
            // println!("{:?}", String::from_utf8_lossy(&v));
            // println!("{:?}", vertx_ha_info_as_node(v, &jvm));
            println!("full time: {:?}", time.elapsed());
        }
        

        // std::thread::park();
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

    use std::sync::Mutex;
    use std::sync::RwLock;
    use jvm_serializable::java::io::*;


    #[jvm_object(io.vertx.core.net.impl.ServerID,5636540499169644934)]
    struct ServerID {
        port: i32,
        host: String
    }


    #[jvm_object(io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo,1)]
    struct ClusterNodeInfo {
        nodeId: String,
        serverID: ServerID,
    }

    #[jvm_object(java.lang.Object,0)]
    struct Object {
        key: String,
        value: String,
    }

    #[jvm_object(io.vertx.spi.cluster.zookeeper.impl.ZKSyncMap$KeyValue,6529685098267757690)]
    struct ZKSyncMapKeyValue {
        key: Object,
    }

    #[test]
    fn zk_test () {  
        use std::time::Duration;
        
        static ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH : &str = "/cluster/nodes";
        static ZK_PATH_HA_INFO : &str = "/syncMap/__vertx.haInfo";
        static ZK_PATH_SUBS : &str = "/asyncMultiMap/__vertx.subs";
        static ZK_ROOT_NODE : &str = "io.vertx.01";


        let kv = ZKSyncMapKeyValue {
            key : Object {
                key: "0e4b0367-c5e6-4559-9284-282f27349de7".to_string(),
                value: "{\"verticles\":[],\"group\":\"__DISABLED__\",\"server_id\":{\"host\":\"localhost\",\"port\":41469}}".to_string()
            }
        };

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

        let mut ha_info_cache = PathChildrenCache::new(zk_arc.clone(), ZK_PATH_HA_INFO).unwrap();
        match ha_info_cache.start() {
            Err(err) => {
                println!("error starting cache: {:?}", err);
                return;
            }
            _ => {
                println!("ha_info_cache started");
            }
        }
        ha_info_cache.add_listener(move |event| {
            match event {
                PathChildrenCacheEvent::ChildAdded(id, data) => {
                    let bytes_of_data = data.0.clone();
                    let bytes_as_string = String::from_utf8_lossy(&bytes_of_data);
                    let idx = bytes_as_string.rfind("$");
                    let bytes_as_string = &bytes_as_string[idx.unwrap()+1..];
                    let mut bytes_as_string_split = bytes_as_string.split("t\u{0}U");
                    let key_value = ZKSyncMapKeyValue {
                        key: Object {
                            key: bytes_as_string_split.next().unwrap().to_string(),
                            value: bytes_as_string_split.next().unwrap().to_string(),
                        }
                    };

                    println!("{:?}", key_value);
                },
                _ => {}
            }
        });


        let mut zk_path_subs = PathChildrenCache::new(zk_arc.clone(), ZK_PATH_SUBS).unwrap();
        zk_path_subs.start().unwrap();
        let zk_ark1 = zk_arc.clone();
        let subs : Arc<Mutex<Vec<Mutex<PathChildrenCache>>>> = Arc::new(Mutex::new(Vec::new())); 
        let clone_subs = subs.clone();

        let (sender, receiver) : (std::sync::mpsc::Sender<String>, std::sync::mpsc::Receiver<String>) = std::sync::mpsc::channel();
        
        let zk_subs = zk_ark1.clone();
        std::thread::spawn(move || {
            loop {

                match receiver.recv() {
                    Ok(recv) => {
                        let inner_subs = Mutex::new(PathChildrenCache::new(zk_subs.clone(), &recv).unwrap());
                        inner_subs.lock().unwrap().start().unwrap();
                        inner_subs.lock().unwrap().add_listener(move |ev| {
                            match ev {
                                PathChildrenCacheEvent::ChildAdded(id, data) => {
                                    println!("{:?}", id);
                                    let data_as_byte = &data.0;
                                    let mut deser = ObjectInputStream{};
                                    let node : ClusterNodeInfo = deser.read_object(data_as_byte.clone());
                                    println!("{:?}", node);
                                    
                                },
                                PathChildrenCacheEvent::ChildRemoved(id) => {

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
            // println!("zk_path_subs event: {:?}", event);
            match event {
                PathChildrenCacheEvent::ChildAdded(_id, _data) => {
                    sender_clone.send(_id.clone()).unwrap();
                },
                _ => {}
            }
        });

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
