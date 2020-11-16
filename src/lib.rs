#![feature(get_mut_unchecked)]
#![feature(fn_traits)]
#![feature(plugin)]
#[allow(non_upper_case_globals)]
extern crate hypospray;
#[macro_use]
extern crate jvm_macro;
extern crate jvm_serializable;


pub mod vertx;

#[cfg(feature = "zk")]
pub mod zk;


