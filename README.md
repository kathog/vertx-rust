# vertx-rust
[crates-badge]: https://img.shields.io/crates/v/vertx-rust
[crates-url]: https://crates.io/crates/vertx-rust

[![Platform](https://img.shields.io/badge/platform-%20%20%20%20Linux-green.svg?style=flat)](https://github.com/kathog/vertx-rust)
[![License](https://img.shields.io/badge/license-%20%20BSD%203%20clause-yellow.svg?style=flat)](LICENSE)
![Rust](https://github.com/kathog/vertx-rust/workflows/Rust/badge.svg)
[![Crates.io][crates-badge]][crates-url]

# Introduction

[vertx-rust](https://github.com/kathog/vertx-rust) its a simple implementation of [vert.x](https://github.com/eclipse-vertx/vert.x) in Rust. The vertx-rust engine is based on [crossbeam-channel](https://github.com/crossbeam-rs/crossbeam-channel) and [tokio](https://github.com/tokio-rs/tokio) as a multi-threaded nonblocking event-driven engine.
Currently, the only implementation is the cluster version based on zookeeper as a cluster manager and standalone instance. In the future there will also be other implementations of the cluster manager

# Features

1. Nonblocking eventbus consumer | local_consumer
2. Nonblocking eventbus request
3. Nonblocking eventbus send
4. Nonblocking eventbus publish
5. Nonblocking multi-threaded tcp server - based on [tokio](https://github.com/tokio-rs/tokio)
6. Nonblocking multi-threaded http server - based on [hyper](https://github.com/hyperium/hyper)
7. Blocking and nonblocking simple [hyper](https://github.com/hyperium/hyper) http client wrapper 
8. Zookeeper cluster manager

# Benchmarks

Benchmarks on AMD Ryzen 7 3800X

## Wrk benchmarks

Http server from **no_cluster** example:
```
wrk -d 90s -t 5 -c 500 http://127.0.0.1:9092/
Running 2m test @ http://127.0.0.1:9092/
  5 threads and 500 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.36ms  715.49us  26.70ms   70.35%
    Req/Sec    74.64k     8.17k   92.34k    59.76%
  33426081 requests in 1.50m, 3.89GB read
Requests/sec: 371207.38
Transfer/sec:     44.25MB
```
Tcp server from **no_cluster** example:
```
wrk -d 90s -t 5 -c 500 http://127.0.0.1:9091/
Running 2m test @ http://127.0.0.1:9091/
  5 threads and 500 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.45ms  768.63us  11.52ms   70.22%
    Req/Sec    70.55k     8.68k    103.19k    75.66%
  31591444 requests in 1.50m, 3.56GB read
Requests/sec: 350878.94
Transfer/sec:     40.49MB
```

## Microbenchmarks
```
serialize_cycles        time:   [260.7716 cycles 261.2144 cycles 261.6987 cycles]           
Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) low mild
  3 (3.00%) high mild
  3 (3.00%) high severe

deserialize_cycles      time:   [325.4613 cycles 325.8329 cycles 326.2337 cycles]             
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

serialize_message       time:   [66.672 ns 67.425 ns 68.939 ns]                              
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

deserialize_message     time:   [70.224 ns 70.403 ns 70.592 ns]                                
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

vertx_request           time:   [4.5056 us 4.5872 us 4.6704 us]                           
Found 6 outliers among 100 measurements (6.00%)
  6 (6.00%) high mild

vertx_send              time:   [274.87 ns 281.62 ns 289.84 ns]                       
Found 8 outliers among 100 measurements (8.00%)
  6 (6.00%) high mild
  2 (2.00%) high severe

vertx_publish           time:   [274.64 ns 279.26 ns 284.22 ns]                          
Found 5 outliers among 100 measurements (5.00%)
  5 (5.00%) high mild

```

# Work with vertx-rust

## Code examples

### Eventbus consumer
```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus().await;

event_bus.consumer("test.01", move |m, _| {
  Box::pin( async {
    m.reply(..);
  })
});

vertx.start().await;
```

### Eventbus send

```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus().await;

event_bus.send("test.01", Body::String("Hello World".to_string()));
```

### Eventbus request

```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus().await;

event_bus.request("test.01", Body::String("Hello World".to_string()), move |m, _| {
    Box::pin(async move {
        ...
    })
});
```

### Tcp server

```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};
use vertx_rust::net::NetServer;

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus().await;

let net_server = NetServer::new(Some(event_bus.clone()));
net_server.listen(9091, move |_req, ev| {
  let mut resp = vec![];
  ...
  resp
});

vertx.start().await;

```

More examples on: [examples](https://github.com/kathog/vertx-rust/tree/main/examples)
