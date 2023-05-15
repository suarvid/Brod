use brod::async_prod;
use brod::async_prod::AsyncFunctionWrapper;
use brod::prod_utils::{self, type_erase_args_vec_async, type_erase_single_arg_async};

use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::task;

use std::any::Any;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const N_MESSAGES: u32 = 1_000;

#[tokio::main()]
async fn main() {
    pretty_env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let n_threads: u32 = args[1].parse().unwrap();
    let topic = "test-topic";
    let bootstrap_server = "localhost:9092";
    let msg_timeout_ms = "5000";

    let producer_config = prod_utils::get_throughput_producer(bootstrap_server, msg_timeout_ms);

    let mut args = Vec::new();

    let messages_per_thread = N_MESSAGES / n_threads;

    // Sequential
    let sequential_prod = prod_utils::get_throughput_producer(bootstrap_server, msg_timeout_ms)
        .create()
        .unwrap();
    let sequential_key = String::from("sequential");
    let sequential_payload = String::from("PAYLOAD");

    let sequential_start = Instant::now();
    produce_sequential(sequential_prod, topic, sequential_key, sequential_payload).await;
    let sequential_elapsed = sequential_start.elapsed();

    // Shared
    let shared_prod = prod_utils::get_throughput_producer(bootstrap_server, msg_timeout_ms)
        .create()
        .unwrap();
    let shared_key = String::from("shared");
    let shared_payload = String::from("PAYLOAD");

    let shared_start = Instant::now();
    produce_shared(
        n_threads,
        shared_prod,
        topic,
        messages_per_thread,
        shared_key,
        shared_payload,
    )
    .await;
    let shared_elapsed = shared_start.elapsed();

    // Wrapper
    args.push(type_erase_single_arg_async(messages_per_thread));
    args.push(type_erase_single_arg_async(String::from("worker")));
    args.push(type_erase_single_arg_async(String::from("PAYLOAD")));

    let async_fn_wrapper = AsyncFunctionWrapper::new(|prod, topic, args| {
        Box::pin(async_worker_function(prod, topic, args))
    });

    let wrapper_start = Instant::now();
    let _results =
        async_prod::produce_in_parallel(n_threads, topic, &producer_config, async_fn_wrapper, args)
            .await;
    let wrapper_elapsed = wrapper_start.elapsed();

    println!(
        "Sequential execution time: {} ms",
        sequential_elapsed.as_millis()
    );

    println!("Shared execution time: {} ms", shared_elapsed.as_millis());

    println!("Wrapper execution time: {} ms", wrapper_elapsed.as_millis());
}

async fn produce_sequential(
    producer: FutureProducer,
    topic: &'static str,
    key: String,
    payload: String,
) {
    let mut futures = Vec::new();

    for _ in 0..N_MESSAGES {
        let future = producer.send(
            FutureRecord::to(topic).key(&key).payload(&payload),
            Duration::from_secs(5),
        );
        futures.push(future);
    }

    for future in futures {
        match future.await {
            Ok(_) => {}
            Err((e, _)) => println!("Failed to produce message: {:#?}", e),
        }
    }
}

async fn produce_shared(
    n_threads: u32,
    producer: FutureProducer,
    topic: &'static str,
    msgs_per_thread: u32,
    key: String,
    payload: String,
) {
    let mut handles = Vec::new();
    let producer = Arc::new(producer);
    for _ in 0..n_threads {
        let payload = payload.clone();
        let key = key.clone();
        let prod_handle = Arc::clone(&producer);
        let handle = task::spawn(async move {
            for _ in 0..msgs_per_thread {
                let res = prod_handle
                    .send(
                        FutureRecord::to(topic).key(&key).payload(&payload),
                        Duration::from_secs(5),
                    )
                    .await;
                match res {
                    Ok(_) => {}
                    Err((e, _)) => println!("Failed to produce message: {:#?}", e),
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

async fn async_worker_function(
    producer: FutureProducer,
    topic: &'static str,
    args: Vec<Arc<dyn Any + Sync + Send>>,
) {
    let n = args[0].clone();
    let n = n.downcast::<u32>().unwrap();

    println!("Messages per thread: {}", n);

    let key = args[1].clone();
    let key = key.downcast_ref::<String>().unwrap();

    let payload = args[2].clone();
    let payload = payload.downcast_ref::<String>().unwrap();

    let mut futures = Vec::new();

    for _ in 0..*n {
        let future = producer.send(
            FutureRecord::to(topic).payload(&payload).key(&key),
            Duration::from_secs(5),
        );

        futures.push(future);
    }

    for future in futures {
        match future.await {
            Ok(_) => {}
            Err((e, _)) => println!("Failed to produce message: {:#?}", e),
        }
    }
}
