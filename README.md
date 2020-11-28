# vertx-rust

[![Platform](https://img.shields.io/badge/platform-%20%20%20%20Linux-green.svg?style=flat)](https://github.com/kathog/vertx-rust)
[![License](https://img.shields.io/badge/license-%20%20BSD%203%20clause-yellow.svg?style=flat)](LICENSE)
[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

# Introduction

[vertx-rust](https://github.com/kathog/vertx-rust) its a simple implementation of [vert.x](https://github.com/eclipse-vertx/vert.x) in Rust. The vertx-rust engine is based on [crossbeam-channel](https://github.com/crossbeam-rs/crossbeam-channel) and [rayon](https://github.com/rayon-rs/rayon) as a multi-threaded nonblocking event-driven engine.
Currently, the only implementation is the cluster version based on zookeeper as a cluster manager and standalone instance. In the future there will also be other implementations of the cluster manager

# Features

1. Nonblocking eventbus consumer | local_consumer
2. Nonblocking eventbus request
3. Nonblocking eventbus send
4. Nonblocking eventbus publish
5. Nonblocking multi-threaded tcp server - based on [tokio](https://github.com/tokio-rs/tokio)
6. Nonblocking multi-threaded http server - based on [hyper](https://github.com/hyperium/hyper)
6. Zookeeper cluster manager

# Benchmarks

Benchmarks on Dell G3 with Intel Core i7-8750H

## Wrk benchmarks

Http server from **no_cluster** example:
```
wrk -d 90s -t 5 -c 500 http://127.0.0.1:9092/
Running 2m test @ http://127.0.0.1:9092/
  5 threads and 500 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.63ms    1.59ms  34.87ms   75.80%
    Req/Sec    38.32k     4.30k   58.83k    70.42%
  17140064 requests in 1.50m, 1.98GB read
Requests/sec: 190307.69
Transfer/sec:     22.50MB
```
Tcp server from **no_cluster** example:
```
wrk -d 90s -t 5 -c 500 http://127.0.0.1:9091/
Running 2m test @ http://127.0.0.1:9091/
  5 threads and 500 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.20ms    2.12ms  38.74ms   92.04%
    Req/Sec    48.20k     7.05k   96.87k    73.34%
  21560750 requests in 1.50m, 2.41GB read
Requests/sec: 239376.03
Transfer/sec:     27.39MB
```

## Microbenchmarks
```
vertx_request           time:   [8.1251 us 8.1982 us 8.2809 us]                           
Found 7 outliers among 100 measurements (7.00%)
  5 (5.00%) high mild
  2 (2.00%) high severe

vertx_send              time:   [1.4308 us 1.4435 us 1.4594 us]                        
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) low severe
  2 (2.00%) low mild
  3 (3.00%) high severe

vertx_publish           time:   [1.8024 us 1.8305 us 1.8527 us]                           

```

# Work with vertx-rust

## Code examples

### Eventbus consumer
```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus();

event_bus.consumer("test.01", move |m, _| {
  m.reply(..);
});

vertx.start();
```

### Eventbus send

```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus();

event_bus.send("test.01", "Hello World".as_bytes().to_vec());
```

### Eventbus request

```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus();

event_bus.request("test.01", "Hello World".as_bytes().to_vec(), move |m, _| {
  ...
});
```

### Tcp server

```rust
use vertx_rust::vertx::{VertxOptions, Vertx, NoClusterManager};
use vertx_rust::net::NetServer;

let vertx_options = VertxOptions::default();
let vertx : Vertx<NoClusterManager> = Vertx::new(vertx_options);
let event_bus = vertx.event_bus();

let net_server = NetServer::new(Some(event_bus.clone()));
net_server.listen(9091, move |_req, ev| {
  let mut resp = vec![];
  ...
  resp
});

vertx.start();

```

More examples on: [examples](https://github.com/kathog/vertx-rust/tree/main/examples)
