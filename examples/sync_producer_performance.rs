use rdkafka::producer::{BaseProducer, BaseRecord};
use std::sync::Arc;
use std::time::Instant;
use std::{any::Any, thread};

use brod::{
    prod_utils::{self, type_erase_single_arg_async},
    sync_prod::produce_in_parallel,
};

use pretty_env_logger;

const N_MESSAGES: usize = 100_000;

/// A basic performance comparison between a using a single producer with one thread,
/// a single producer shared between multiple threads, and multiple threads each alloted
/// its own producer instance, using the synchronous wrapper function.
/// In each case, a total of 100 000 messages are sent, each containing a short string
/// as both the key and the payload.
fn main() {
    pretty_env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let n_threads: usize = args[1].parse().unwrap();
    let topic = "test-topic";

    let producer_config = prod_utils::get_throughput_producer("localhost:9092", "100");

    let mut args = Vec::new();

    let n_messages_per_thread = N_MESSAGES / n_threads;

    // Should be n, key, payload
    args.push(type_erase_single_arg_async(n_messages_per_thread));
    args.push(type_erase_single_arg_async(String::from("worker")));
    args.push(type_erase_single_arg_async(String::from("PAYLOAD")));

    let wrapper_start = Instant::now();
    match produce_in_parallel(6, topic, &producer_config, send_n_messages_worker, args) {
        Ok(_) => println!("Successfully produced messages!"),
        Err(e) => println!("Failed to produce messages: {:?}", e),
    }
    let wrapper_elapsed = wrapper_start.elapsed();

    let seq_prod: &BaseProducer = &producer_config.create().unwrap();

    let key = String::from("sequential");
    let payload = String::from("PAYLOAD");
    let sequential_start = Instant::now();
    send_n_messages_sequential(seq_prod, topic, N_MESSAGES, key, payload);
    let sequential_elapsed = sequential_start.elapsed();

    let shared_prod = producer_config.create().unwrap();
    let key = String::from("parallel");
    let payload = String::from("PAYLOAD");

    let shared_start = Instant::now();
    send_n_messages_shared_producer(n_threads, shared_prod, topic, N_MESSAGES, key, payload);
    let shared_elapsed = shared_start.elapsed();

    println!(
        "Time elapsed for sequential execution: {} ms",
        sequential_elapsed.as_millis()
    );

    println!(
        "Time elapsed for parallel execution with a shared instance: {} ms",
        shared_elapsed.as_millis()
    );

    println!(
        "Time elapsed for parallel execution with wrapper function: {} ms",
        wrapper_elapsed.as_millis()
    );
}

/// Sends messages using a single thread and single producer instance
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

/// Sends messages using a single producer instance, shared among multiple threads
fn send_n_messages_shared_producer(
    n_threads: usize,
    producer: BaseProducer,
    topic: &'static str,
    n: usize,
    key: String,
    payload: String,
) {
    let mut handles = Vec::new();
    let shared_prod = Arc::new(producer);

    for _ in 0..n_threads {
        let prod = Arc::clone(&shared_prod);
        let payload = payload.clone();
        let key = key.clone();
        let handle = thread::spawn(move || {
            for _ in 0..n {
                let send_res = prod.send(BaseRecord::to(topic).key(&key).payload(&payload));
                match send_res {
                    Ok(_) => {}
                    Err(e) => {} //println!("Failed to produce message: {:#?}", e),
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

/// Worker function which is passed to the wrapper function provided by the library,
/// in order to execute the function in parallel.
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
