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
        struct ClusterNodeInfo {
            node_id: String,
            server_id: ServerId
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

        fn vertx_subs_as_bytes (node: ClusterNodeInfo, jvm : &Jvm) -> Vec<u8> {
            let service_id = jvm.create_instance("io.vertx.core.net.impl.ServerID", &vec![
                InvocationArg::try_from(node.server_id.port as i32).unwrap().into_primitive().unwrap(),
                InvocationArg::try_from(node.server_id.host).unwrap()
                ]).unwrap();

            let cluster_node_id = jvm.create_instance("io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo", &vec![
                InvocationArg::try_from(node.node_id).unwrap(),
                InvocationArg::from(service_id)
                ]).unwrap();

            let node_id_as_bytes = jvm.invoke_static("eu.craftsoft.NodeInfoSerializer", "fromNodeInfo", &vec![InvocationArg::from(cluster_node_id)]);
            let node_id_as_rust_bytes = jvm.to_rust::<String>(node_id_as_bytes.unwrap()).unwrap();
            return node_id_as_rust_bytes.from_base64().unwrap();
        }

        fn vertx_subs_as_node(data : Vec<u8>, jvm : &Jvm) -> ClusterNodeInfo {
            let data_as_i8 : Vec<InvocationArg> = data.into_iter().map(|x| InvocationArg::try_from( x as i8).unwrap()).collect();
            let data_as_java_array = jvm.create_java_array("java.lang.Byte", &data_as_i8).unwrap();
            let to_key_value = jvm.invoke_static("eu.craftsoft.NodeInfoSerializer", "toNodeInfo", &vec![InvocationArg::from(data_as_java_array)]);
            let result_as_instance = to_key_value.unwrap();
            let node_id_field = jvm.field(&result_as_instance, "nodeId").unwrap();
            let node_id_as_rust = jvm.to_rust::<String>(node_id_field).unwrap();
            
            let service_id_field = jvm.field(&result_as_instance, "serverID").unwrap();
            let port_field = jvm.field(&service_id_field, "port").unwrap();
            let host_field = jvm.field(&service_id_field, "host").unwrap();
            let port_as_rust = jvm.to_rust::<i32>(port_field).unwrap();
            let host_as_rust = jvm.to_rust::<String>(host_field).unwrap();

            return ClusterNodeInfo {
                node_id : node_id_as_rust,
                server_id : ServerId {
                    host: host_as_rust,
                    port: port_as_rust as usize
                }
            };
        }


    #[test]
    fn serialize_test () { 
        let time = std::time::Instant::now();
        let entry = ClasspathEntry::new("/home/nerull/dev/repos/java/node-info-serializer/target/node-info-serializer-1.0.0-fat.jar");
        let jvm: Jvm = JvmBuilder::new().classpath_entry(entry).build().unwrap();
        println!("init jvm time: {:?}", time.elapsed());

        for i in 0..1 {
            let time = std::time::Instant::now();

            let node = ClusterNodeInfo {
                node_id : "21a8a87e-55c6-4341-a3b2-67567c9cacfc".to_owned(),
                server_id : ServerId {
                    host: "localhost".to_owned(),
                    port: 45937
                }
            };

            let node_info = NodeInfo {
                verticles : vec![],
                group: "__DISABLED__".to_owned(),
                server_id : ServerId {
                    host: "localhost".to_owned(),
                    port: 45937
                }
            };

            let subs_as_bytes = vertx_subs_as_bytes(node, &jvm);
            // println!("{:?}", String::from_utf8_lossy(&subs_as_bytes));
            println!("{:?}",  vertx_subs_as_node(subs_as_bytes, &jvm));



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


    

    #[test]
    fn zk_test () {  
        use std::time::Duration;
        
        static ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH : &str = "/cluster/nodes";
        static ZK_PATH_HA_INFO : &str = "/syncMap/__vertx.haInfo";
        static ZK_PATH_SUBS : &str = "/asyncMultiMap/__vertx.subs";
        static ZK_ROOT_NODE : &str = "io.vertx.01";
        

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

        let jvm: Jvm = JvmBuilder::new()
                .with_maven_settings(MavenSettings::new(vec![
                    MavenArtifactRepo::from("repsy::https://repo.repsy.io/mvn/nerull/default")])
                ).build().unwrap();

            jvm.deploy_artifact(&MavenArtifact::from("eu.craftsoft:node-info-serializer:1.0.0")).unwrap();
            jvm.deploy_artifact(&MavenArtifact::from("io.vertx:vertx-zookeeper:3.6.3")).unwrap();
            jvm.deploy_artifact(&MavenArtifact::from("io.vertx:vertx-core:3.9.4")).unwrap();


        let mutex_jvm = Arc::new(Mutex::new(jvm));    
        let clone_jvm = mutex_jvm.clone();

        let zk_ark0 = zk_arc.clone();
        // pcc.add_listener(move |event| {
        //     let time = std::time::Instant::now();
        //     let jvm: Jvm = JvmBuilder::new()
        //         .with_maven_settings(MavenSettings::new(vec![
        //             MavenArtifactRepo::from("repsy::https://repo.repsy.io/mvn/nerull/default")])
        //         ).build().unwrap();

        //     jvm.deploy_artifact(&MavenArtifact::from("eu.craftsoft:node-info-serializer:1.0.0")).unwrap();
        //     jvm.deploy_artifact(&MavenArtifact::from("io.vertx:vertx-zookeeper:3.6.3")).unwrap();
        //     jvm.deploy_artifact(&MavenArtifact::from("io.vertx:vertx-core:3.9.4")).unwrap();
        //     println!("jvm init: {:?}", time.elapsed());
        //     match event {
        //         PathChildrenCacheEvent::ChildAdded(_id, name) => {
        //             println!("ChildAdded id: {:?}, name: {:?}", _id.replace("/cluster/nodes/", ""), std::str::from_utf8(&name.0));
        //             std::thread::sleep(std::time::Duration::from_micros(10));
        //             match std::str::from_utf8(&name.0) {
        //                 Ok(node_id) => {
        //                     match zk_ark0.get_data(&format!("{}/{}", ZK_PATH_HA_INFO, node_id), false) {
        //                         Ok(data) => {
        //                             let node_tuple = vertx_ha_info_as_node(data.0, &jvm);
        //                             println!("node id: {:?}, value: {:?}", node_tuple.0, node_tuple.1);
        //                             match zk_ark0.get_children(&format!("{}", ZK_PATH_SUBS), false) {
        //                                 Ok(data ) => {
        //                                     println!("event.buses: {:?}", data);
        //                                     for event_bus in data {
        //                                         match zk_ark0.get_data_w(&format!("{}/{}/{}:{}:{}", ZK_PATH_SUBS, event_bus, node_id, node_tuple.1.server_id.host, node_tuple.1.server_id.port), LoggingWatcher) {
        //                                             Ok(data) => {
        //                                                 println!("__vertx.subs: {:?}", vertx_subs_as_node(data.0, &jvm));
        //                                             },
        //                                             Err(_) => {}
        //                                         }
        //                                     }
        //                                 },
        //                                 Err(_) => {}
        //                             }     
        //                         },
        //                         Err(_) => {}
        //                     }                                               
        //                 },
        //                 Err(_) => {}
        //             }
        //         },
        //         PathChildrenCacheEvent::ChildRemoved(id) => {
        //             println!("ChildRemoved: {:?}", id.replace("/cluster/nodes/", ""));
        //         },
        //         _ => {}
        //     }
        // });

        // let mut ha_info_cache = PathChildrenCache::new(zk_arc.clone(), ZK_PATH_HA_INFO).unwrap();
        // match ha_info_cache.start() {
        //     Err(err) => {
        //         println!("error starting cache: {:?}", err);
        //         return;
        //     }
        //     _ => {
        //         println!("ha_info_cache started");
        //     }
        // }
        // ha_info_cache.add_listener(move |event| {
        //     println!("ha_info_cache event: {:?}", event);
        // });


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
                        // println!("inner_subs path: {:?}", &recv);
                        let inner_subs = Mutex::new(PathChildrenCache::new(zk_subs.clone(), &recv).unwrap());
                        inner_subs.lock().unwrap().start().unwrap();
                        inner_subs.lock().unwrap().add_listener(move |ev| {
                            match ev {
                                PathChildrenCacheEvent::ChildAdded(id, data) => {
                                    let data_as_byte = &data.0;
                                    let mut copy_data = Vec::with_capacity(data_as_byte.len());
                                    for byte in data_as_byte {
                                        copy_data.push(*byte);
                                    }
                                    println!("inner_subs event: {:?}", String::from_utf8_lossy(&copy_data));
                                    //idx 0: flaga bool 
                                    let cluster_serializable = data_as_byte[0] == 1;
                                    println!("{:?}", cluster_serializable);
                                    println!("{:?}", &data_as_byte[191..]);

                                    //stream header
                                    //STREAM_MAGIC
                                    println!("{}", i16::from_be_bytes(copy_data[1..3].try_into().unwrap()));
                                    //STREAM_VERSION
                                    println!("{}", i16::from_be_bytes(copy_data[3..5].try_into().unwrap()));
                                    
                                    
                                    //idx 5: TC_OBJECT
                                    //idx 6: TC_CLASSDESC 
                                    //idx 7-8: class name size
                                    //class name
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[9..62]));
                                    //serialVersionUID
                                    // let serialVersionUID : [u8;8] = [copy_data[62], copy_data[63], copy_data[64], copy_data[65], copy_data[66], copy_data[67], copy_data[68], copy_data[69]];
                                    // let (int_bytes, rest) = copy_data[62..70].split_at(std::mem::size_of::<i64>());
                                    println!("{:?}", i64::from_be_bytes(copy_data[62..70].try_into().unwrap()));
                                    //class flags
                                    println!("{:?}", copy_data[70]);
                                    //fields len
                                    println!("{:?}", i16::from_be_bytes(copy_data[71..73].try_into().unwrap()));

                                    // fields loop
                                    // field type:
                                    println!("{:?}", copy_data[73]);
                                    // nodeId
                                    // field name len
                                    let f_len = i16::from_be_bytes(copy_data[74..76].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    // field name
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[76..(76+f_len) as usize]));
                                    //type string
                                    // TC_CLASS
                                    println!("{:?}", copy_data[82]);
                                    let f_len = i16::from_be_bytes(copy_data[83..85].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    // field name
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[86..(86+f_len) as usize]));
                                    
                                    //serverID
                                    // field name len
                                    let f_len = i16::from_be_bytes(copy_data[104..106].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    // field name
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[106..(106+f_len) as usize]));
                                    // TC_CLASS
                                    println!("{:?}", copy_data[114]);
                                    let f_len = i16::from_be_bytes(copy_data[115..117].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    // field name
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[117..(117+f_len) as usize]));
                                    
                                    //TC_ENDBLOCKDATA
                                    println!("{:?}", copy_data[150]);
                                    //TC_NULL
                                    println!("{:?}", copy_data[151]);

                                    //nodeId , TC_CLASS
                                    println!("{:?}", copy_data[152]);
                                    let f_len = i16::from_be_bytes(copy_data[153..155].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    // field value
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[155..(155+f_len) as usize]));
                                    println!("act {:?}", (155+f_len));

                                    println!("{:?}", copy_data[191]);
                                    println!("{:?}", copy_data[192]);

                                    let f_len = i16::from_be_bytes(copy_data[193..195].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    println!("class size {:?}", f_len);
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[195..(195+f_len) as usize]));
                                    
                                    //serial uuid
                                    println!("{:?}", i64::from_be_bytes(copy_data[226..234].try_into().unwrap()));
                                    //class flags
                                    println!("{:?}", copy_data[234]);
                                    //fields len
                                    println!("{:?}", i16::from_be_bytes(copy_data[235..237].try_into().unwrap()));
                                    


                                    // fields loop
                                    //port
                                    // field type:
                                    println!("{:?}", copy_data[237]);
                                    // field name len
                                    let f_len = i16::from_be_bytes(copy_data[238..240].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    // field name
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[240..(240+f_len) as usize]));
                                    println!("act {:?}", (240+f_len));
                                    
                                    //host
                                    // field type:
                                    println!("{:?}", copy_data[244]);
                                    // field name len
                                    let f_len = i16::from_be_bytes(copy_data[245..247].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    // field name
                                    println!("{:?}", String::from_utf8_lossy(&copy_data[247..(247+f_len) as usize]));
                                    println!("act {:?}", (247+f_len));
                                    // TC_REFERENCE
                                    println!("{:?}", copy_data[251]);
                                    let f_len = i32::from_be_bytes(copy_data[252..256].try_into().unwrap());
                                    println!("{:?}", f_len);

                                     //TC_ENDBLOCKDATA
                                     println!("{:?}", copy_data[256]);
                                     //TC_NULL
                                     println!("{:?}", copy_data[257]);

                                     let f_len = i32::from_be_bytes(copy_data[258..262].try_into().unwrap());
                                    println!("{:?}", f_len);
                                    
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
