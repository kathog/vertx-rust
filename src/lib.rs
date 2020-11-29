#![feature(get_mut_unchecked)]
#![feature(fn_traits)]
#![feature(plugin)]
#![feature(type_name_of_val)]
#[cfg(feature = "tc")]
extern crate tcmalloc;
#[cfg(feature = "tc")]
use tcmalloc::TCMalloc;
#[cfg(feature = "tc")]
#[global_allocator]
pub static ALLOCATOR: TCMalloc = TCMalloc;

#[cfg(not(feature = "tc"))]
use std::alloc::System;
#[cfg(not(feature = "tc"))]
#[global_allocator]
pub static ALLOCATOR: System = System;

#[allow(non_upper_case_globals)]
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
