use std::time::Duration;
use std::{any::Any, sync::Arc};

use brod::async_prod;
use brod::async_prod::AsyncFunctionWrapper;
use brod::prod_utils;
use rdkafka::producer::FutureProducer;

#[tokio::main]
async fn main() {

    // This is a basic example showing how to use the async producer.

    let topic = "test-topic";
    let num_threads = 4;

    // Default producer config, with bootstrap servers set to localhost:9092
    // and message timeout set to 10ms.
    let producer_config = prod_utils::get_default_producer_config("localhost:9092", "10");

    let mut args = Vec::new();
    args.push(Arc::new(1) as Arc<dyn Any + Sync + Send>);
    args.push(Arc::new(String::from("key")) as Arc<dyn Any + Sync + Send>);

    // Creating a wrapper for an async worker function is currently slightly complex,
    // requires a closure that returns a boxed future.
    let async_fn_wrapper = AsyncFunctionWrapper::new(|prod, topic, args| {
        Box::pin(basic_async_worker_function(prod, topic, args))
    });

    let results = async_prod::produce_in_parallel(
        num_threads,
        topic,
        &producer_config,
        async_fn_wrapper,
        args,
    )
    .await;

    match results {
        Ok(results) => {
            println!("Got {} results", results.len());
            for result in results {
                println!("Got result: {}", result);
            }
        }
        Err(e) => {
            println!("Got error: {:#?}", e);
        }
    }
}

async fn basic_async_worker_function(
    producer: FutureProducer,
    topic: &'static str,
    args: Vec<Arc<dyn Any + Sync + Send>>,
) -> String {
    println!("Hello from the async worker function!");
    println!("Got {} arguments", args.len());

    // Here you would do some work and send some messages with the producer.
    // For example:
    let message = args[0].clone();

    let key = args[1].clone();

    // Do some work with the message, for example, downcast it to the type you expect.
    let message = message.downcast_ref::<i32>().unwrap();

    // Any data sent to the worker function needs to implement the ToBytes trait.
    // A basic solution is to use to_be_bytes() or to_le_bytes() to convert the data
    // For custom structs, you can implement the ToBytes trait yourself.
    let message_bytes = message.to_be_bytes();

    // Do some work with the key, for example, downcast it to the type you expect.
    let key = key.downcast_ref::<String>().unwrap();

    // The downcast_ref could fail, so you should handle that case.

    // Send the message with the producer.
    let send_res = producer
        .send(
            rdkafka::producer::FutureRecord::to(topic)
                .payload(&message_bytes)
                .key(key),
            Duration::from_secs(1),
        )
        .await;

    match send_res {
        Ok(_) => println!("Successfully sent message!"),
        Err(e) => println!("Error sending message: {:#?}", e),
    }

    String::from("this is returned from the worker function")
}
