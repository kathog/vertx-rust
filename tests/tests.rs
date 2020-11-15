#[macro_use]
extern crate jvm_macro;
extern crate jvm_serializable;
#[macro_use]
extern crate lazy_static;

#[cfg(test)]
mod tests {

    #[path = "../src/lib.rs"]
    extern crate vertx_rust;
    use vertx_rust::vertx::*;
    use std::sync::Arc;
    use std::convert::TryFrom;
    use rustc_serialize::base64::FromBase64;
    use core::convert::TryInto;
    use serde_json::*;
    use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper};
    use zookeeper::recipes::cache::*;
    use serde_json::*;
    use serde::{Serialize, Deserialize};
    use std::sync::Mutex;
    use std::sync::RwLock;
    use jvm_serializable::java::io::*;
    use futures::StreamExt;
    use tokio::net::TcpListener;
    use tokio::prelude::*;


    struct LoggingWatcher;
    impl Watcher for LoggingWatcher {
        fn handle(&self, e: WatchedEvent) {
            println!("{:?}", e);
        }
    }



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

        pcc.add_listener(move |event| {
           println!("{:?}", event);
        });

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