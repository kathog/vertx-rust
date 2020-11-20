#![feature(get_mut_unchecked)]
#![feature(fn_traits)]
#![feature(plugin)]

#[cfg(feature = "tc")]
extern crate tcmalloc;
#[cfg(feature = "tc")]
use tcmalloc::TCMalloc;
#[cfg(feature = "tc")]
#[global_allocator]
static GLOBAL: TCMalloc = TCMalloc;


#[allow(non_upper_case_globals)]
extern crate hypospray;
#[macro_use]
extern crate jvm_macro;
extern crate jvm_serializable;
#[macro_use]
extern crate lazy_static;


pub mod vertx;
pub mod net;

#[cfg(feature = "zk")]
pub mod zk;


