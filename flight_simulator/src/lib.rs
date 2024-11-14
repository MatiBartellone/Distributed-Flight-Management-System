pub mod cassandra_comunication;
pub mod flight_implementation;
pub mod thread_pool;
pub mod utils;

use rand::Rng;

pub fn gen_random(a: f64, b: f64) -> f64 {
    let mut r = rand::thread_rng();
    r.gen_range(a..=b)
}
