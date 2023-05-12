// A performance comparsion between executing one thread with one producer instance,
// several threads with one producer instance, and several threads with a producer pool.
// What is a good performance comparison? Sending a fixed number of messages of some size maybe?

use brod::prod_utils;
use brod::producerpool::SyncProducerPool;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::ClientConfig;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const NUM_MESSAGES: usize = 1_000_000;

fn send_messages_prod_pool(pool: SyncProducerPool, topic: &'static str) {
    let n_cores = num_cpus::get();
    let messages_per_core = NUM_MESSAGES / n_cores;

    let pool = Arc::new(pool);

    let mut handles = Vec::new();

    for _ in 0..n_cores {
        let pool = Arc::clone(&pool);
        let topic = topic.clone();

        let handle = thread::spawn(move || {
            for _ in 0..messages_per_core {
                let key = String::from("pool");
                let payload = String::from("PAYLOAD");

                let res = pool.publish(topic, key, payload);
                match res {
                    Ok(_) => {}
                    Err(e) => println!("Error Sending Messages: {:?}", e),
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn send_messages_parallel(prod_config: &ClientConfig, topic: &'static str) {
    let producer: BaseProducer = prod_config.create().unwrap();
    //let safe_producer = Mutex::new(producer);
    let shared_producer = Arc::new(producer);
    let num_threads = num_cpus::get();

    let messages_per_core = NUM_MESSAGES / num_threads;

    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let shared_producer = Arc::clone(&shared_producer);
        let topic = topic.clone();
        let handle = thread::spawn(move || {
            for _ in 0..messages_per_core {
                let key = String::from("parallel");
                let payload = String::from("PAYLOAD");
                match shared_producer.send(BaseRecord::to(topic).key(&key).payload(&payload)) {
                    Ok(_) => {}
                    Err(e) => println!("Error Sending Message: {:?}", e),
                }
            }
        });

        handles.push(handle);
    }

    shared_producer.flush(Duration::from_secs(5));

    for handle in handles {
        handle.join().unwrap();
    }
}

fn send_messages_synchronous(producer: &BaseProducer, topic: &str) {
    for _ in 0..NUM_MESSAGES {
        let key = String::from("key");
        let payload = String::from("PAYLOAD");
        let send_res = producer.send(BaseRecord::to(topic).key(&key).payload(&payload));
        match send_res {
            Ok(_) => {}
            Err(e) => println!("Error Sending Message: {:?}", e),
        }
    }

    producer.flush(Duration::from_secs(5));
}

fn main() {
    let topic = "test-topic";

    //let prod_config = &prod_utils::get_default_producer_config("localhost:9092", "100");
    let prod_config = &prod_utils::get_throughput_producer("localhost:9092", "10");

    match SyncProducerPool::new_with_n_producers(2, prod_config) {
        Ok(pool) => {
            let start_pool = Instant::now();
            send_messages_prod_pool(pool, topic);
            let elapsed_pool = start_pool.elapsed();
            println!(
                "Time elapsed with producer pool: {} ms",
                elapsed_pool.as_millis()
            );
        }
        Err(e) => panic!("Failed to create producer pool: {}", e),
    }

    let start_parallel = Instant::now();
    send_messages_parallel(prod_config, topic);
    let elapsed_parallel = start_parallel.elapsed();
    println!(
        "Time elapsed with multiple threads, one producer: {} ms",
        elapsed_parallel.as_millis()
    );

    let sync_prod: &BaseProducer = &prod_config.create().unwrap();

    let start_synchronous = Instant::now();
    send_messages_synchronous(sync_prod, topic);
    let elapsed_synchronous = start_synchronous.elapsed();
    println!(
        "Time elapsed with synchronous execution: {} ms",
        elapsed_synchronous.as_millis()
    );
}
