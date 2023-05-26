use std::time::Instant;

use brod::prod_utils;
use brod::produce_parallel;
use rdkafka::producer::BaseProducer;
use rdkafka::producer::BaseRecord;

const N_MESSAGES: usize = 1_000_000;

fn publish_some_stuff(
    prod: BaseProducer,
    topic: &str,
    n_msgs: usize,
    key: String,
    message: String,
) -> u32 {
    for i in 0..n_msgs {
        match prod.send(BaseRecord::to(topic).key(&key).payload(&message)) {
            Ok(_) => {}
            Err(_) => println!("Failed to send message"),
        }
    }

    1337
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_threads: usize = args[1].parse().unwrap();
    //let num_threads = 4;
    let topic = "test-topic";

    let producer_config = &prod_utils::get_throughput_producer("localhost:9092", "100");

    let msgs_per_thread = N_MESSAGES / num_threads;
    let macro_start = Instant::now();
    let _results = produce_parallel!(num_threads; producer_config; BaseProducer; publish_some_stuff,
        topic, msgs_per_thread, String::from("macro"), String::from("PAYLOAD"));
    let macro_elapsed = macro_start.elapsed().as_millis();

    println!(
        "Time elapsed when executing with macro: {} ms",
        macro_elapsed
    );
}
