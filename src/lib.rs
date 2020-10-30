#![feature(get_mut_unchecked)]
#![feature(fn_traits)]
#[macro_use]
extern crate lazy_static;

#[cfg(test)]
mod tests {

    use crate::vertx;
    use vertx::*;
    use std::sync::Arc;

    #[test]
    fn it_works() {

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
        for i in 0..1000000 {
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
            let vertx_port: u16 = rng.gen_range(32000, 48000);
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
                            let mut inner_cf = local_cf.clone();

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
                                        let callback = unsafe {
                                            Arc::get_mut_unchecked(&mut inner_cf).lock().unwrap().remove(&address)

                                        };
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
            let mut message = Message {
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
            let mut message = Message {
                address: Some(addr.clone()),
                callback_address: format!("__vertx.reply.{}", uuid::Uuid::new_v4().to_string()),
                body: Arc::new(body),
            };
            unsafe {
                let mut local_cons = self.callback_functions.clone();
                let map = Arc::get_mut_unchecked(&mut local_cons);
                map.lock().unwrap().insert(message.callback_address.clone(), Box::new(op));
            }
            let local_sender = self.sender.lock().unwrap().clone();
            local_sender.send(message).unwrap();
        }

    }

}
