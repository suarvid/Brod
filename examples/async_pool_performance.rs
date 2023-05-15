use std::sync::Arc;
use std::time::Instant;

use tokio::task;

use brod::{prod_utils, producerpool::AsyncProducerPool};

const NUM_MESSAGES: usize = 1_000;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_threads: usize = args[1].parse().unwrap();
    let topic = "test-topic";
    let bootstrap_server = "localhost:9092";
    let msg_timeout_ms = "1000";

    let prod_config = prod_utils::get_throughput_producer(bootstrap_server, msg_timeout_ms);

    match AsyncProducerPool::new(num_threads, &prod_config) {
        Ok(pool) => {
            let start_pool = Instant::now();
            send_msgs_pool(pool, topic, num_threads).await;
            let pool_elapsed = start_pool.elapsed();
            println!("Pool Execution Time: {} ms", pool_elapsed.as_millis());
        }
        Err(e) => panic!("Failed to create producer pool: {}", e),
    }
}

async fn send_msgs_pool(pool: AsyncProducerPool, topic: &'static str, num_threads: usize) {
    let msgs_per_thread = NUM_MESSAGES / num_threads;

    let pool = Arc::new(pool);

    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let pool = Arc::clone(&pool);
        let topic = topic.clone();

        let handle = task::spawn(async move {
            let mut errors = Vec::new();
            for _ in 0..msgs_per_thread {
                let key = String::from("pool");
                let payload = String::from("PAYLOAD");
                let r = pool.publish(topic, key, payload).await;
                if let Err(e) = r {
                    errors.push(e)
                }
            }
            errors
        });
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(errors) => {
                for error in errors {
                    println!("Failed to produce message: {:#?}", error);
                }
            }
            Err(e) => println!("Failed to join thread handle: {:#?}", e),
        }
    }
}
