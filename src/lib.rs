#![feature(get_mut_unchecked)]
#![feature(fn_traits)]
#![feature(type_name_of_val)]
#![feature(array_methods)]
#![allow(non_upper_case_globals)]

#[global_allocator]
static GLOBAL: mimalloc_rust::GlobalMiMalloc = mimalloc_rust::GlobalMiMalloc;

extern crate hypospray;
#[macro_use]
extern crate jvm_macro;
extern crate jvm_serializable;
#[macro_use]
extern crate lazy_static;

pub mod http;
pub mod net;
pub mod vertx;

#[cfg(feature = "zk")]
pub mod zk;
