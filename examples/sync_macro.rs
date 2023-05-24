use brod::prod_utils;
use brod::spawn_threads_with_args;

fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    let num_threads = 4;
    let topic = "test-topic";

    let producer_config = &prod_utils::get_default_producer_config("localhost:9092", "100");

    let res: Result<Vec<i32>, String> =
        spawn_threads_with_args!(num_threads; producer_config; add, 1, 2);

    for r in res.unwrap() {
        println!("r: {}", r);
    }
}
