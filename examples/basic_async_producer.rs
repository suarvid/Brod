use std::{sync::Arc, any::Any};

use parallel_kafka_producer::async_prod::AsyncFunctionWrapper;
use parallel_kafka_producer::prod_utils;
use parallel_kafka_producer::async_prod;
use rdkafka::producer::FutureProducer;

#[tokio::main]
async fn main() {
    let topic = "test-topic";
    let num_threads = 4;

    // Default producer config, with bootstrap servers set to localhost:9092
    // and message timeout set to 10ms.
    let producer_config = prod_utils::get_default_producer_config("localhost:9092", "10");

    let future_producer: FutureProducer = producer_config.create().unwrap();

    let mut args = Vec::new();
    args.push(Arc::new(1) as Arc<dyn Any + Sync + Send>);
    args.push(Arc::new(2) as Arc<dyn Any + Sync + Send>);
    args.push(Arc::new(3) as Arc<dyn Any + Sync + Send>);

    let async_fn_wrapper = AsyncFunctionWrapper::new(|future_producer, topic, args| {
        Box::pin(basic_async_worker_function(future_producer, topic, args))
    });

    let results = async_prod::produce_in_parallel(
        num_threads,
        topic,
        &producer_config,
        async_fn_wrapper,
        args,
    ).await;


}

async fn basic_async_worker_function(
    producer: FutureProducer,
    topic: &'static str,
    args: Vec<Arc<dyn Any + Sync + Send>>,
) -> String {

    println!("Hello from the async worker function!");
    println!("Got {} arguments", args.len());


    String::from("this is returned from the worker function")
}