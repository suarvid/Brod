use rdkafka::producer::{BaseProducer, BaseRecord};
use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use brod::{
    prod_utils::{self, type_erase_single_arg_async},
    sync_prod::produce_in_parallel,
};

use pretty_env_logger;

const N_MESSAGES: usize = 1_000_000;

fn main() {
    pretty_env_logger::init();
    let topic = "test-topic";

    let producer_config = prod_utils::get_throughput_producer("localhost:9092", "100");

    let mut args = Vec::new();

    let n_threads = 32;
    let n_messages_per_thread = N_MESSAGES / n_threads;

    // Should be n, key, payload
    args.push(type_erase_single_arg_async(n_messages_per_thread));
    args.push(type_erase_single_arg_async(String::from("worker")));
    args.push(type_erase_single_arg_async(String::from("PAYLOAD")));

    let parallel_start = Instant::now();
    match produce_in_parallel(6, topic, &producer_config, send_n_messages_worker, args) {
        Ok(_) => println!("Successfully produced messages!"),
        Err(e) => println!("Failed to produce messages: {:?}", e),
    }
    let parallel_elapsed = parallel_start.elapsed();
    println!(
        "Time elapsed for parallel execution: {} ms",
        parallel_elapsed.as_millis()
    );

    let seq_prod: &BaseProducer = &producer_config.create().unwrap();

    let key = String::from("sequential");
    let payload = String::from("PAYLOAD");
    let sequential_start = Instant::now();
    send_n_messages_sequential(seq_prod, topic, N_MESSAGES, key, payload);
    let sequential_elapsed = sequential_start.elapsed();

    println!(
        "Time elapsed for sequential execution: {} ms",
        sequential_elapsed.as_millis()
    );
}

fn send_n_messages_sequential(
    producer: &BaseProducer,
    topic: &'static str,
    n: usize,
    key: String,
    payload: String,
) {
    for _ in 0..n {
        let res = producer.send(BaseRecord::to(topic).key(&key).payload(&payload));
        match res {
            Ok(_) => {}
            Err(e) => println!("Failed to produce message: {:?}", e),
        }
    }
}

fn send_n_messages_worker(
    producer: &BaseProducer,
    topic: &'static str,
    args: Vec<Arc<dyn Any + Sync + Send>>,
) {
    let n = args[0].clone();
    let n = n.downcast::<usize>().unwrap();

    let key = args[1].clone();
    let key = key.downcast_ref::<String>().unwrap();

    let payload = args[2].clone();
    let payload = payload.downcast_ref::<String>().unwrap();

    for _ in 0..*n {
        let res = producer.send(BaseRecord::to(topic).key(key).payload(payload));
        match res {
            Ok(_) => {}
            Err(e) => println!("Failed to produce message: {:?}", e),
        }
    }
}
