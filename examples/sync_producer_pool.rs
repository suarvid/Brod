// Example which shows how a SyncProducerPool can be used

use std::sync::Arc;
use std::thread;

use brod::prod_utils;
use brod::producerpool;
use brod::producerpool::SyncProducerPool;

fn main() {
    let topic = "test-topic";
    let prod_config = &prod_utils::get_default_producer_config("localhost:9092", "10");

    match producerpool::SyncProducerPool::new(prod_config) {
        Ok(pool) => produce_with_pool(pool, topic),
        Err(e) => panic!(
            "Failed to create producer pool with given configuration: {}",
            e
        ),
    }
}

fn produce_with_pool(pool: SyncProducerPool, topic: &'static str) {
    let n_cores = num_cpus::get();

    println!(
        "Using {} cores, pool will contain {} producer instances",
        n_cores, n_cores
    );

    let pool = Arc::new(pool);
    let mut handles = Vec::new();

    for i in 0..n_cores {
        let pool = Arc::clone(&pool);
        let topic = topic.clone();

        let handle = thread::spawn(move || {
            for _ in 0..5 {
                let payload = format!("Hello from thread {}", i);
                let key = format!("{}", i);
                // So each thread publishes five messages

                let res = pool.publish(topic, key, payload);
                if let Err(e) = res {
                    let err_msg = format!("{}", e);
                    panic!("Failed to produce with producer pool: {}", err_msg);
                } else {
                    println!("Successfully produced message!");
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
