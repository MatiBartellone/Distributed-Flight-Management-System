pub mod utils;
pub mod cassandra_client;
pub mod flight;


use rand::Rng;

pub fn gen_random(a: f64, b: f64) -> f64 {
    let mut r = rand::thread_rng();
    r.gen_range(a..=b)
}