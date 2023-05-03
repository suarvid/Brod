use std::{any::Any, sync::Arc, time::Duration};

use parallel_kafka_producer::prod_utils;
use parallel_kafka_producer::sync_prod::produce_in_parallel;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

fn main() {
    let num_threads = 4;
    let topic = "test-topic";

    // Default producer config, with bootstrap servers set to localhost:9092
    // and message timeout set to 10ms.
    let producer_config = prod_utils::get_default_producer_config("localhost:9092", "10");

    // Dumy args vector
    let args = Vec::new();

    match produce_in_parallel(
        num_threads,
        topic,
        &producer_config,
        basic_sync_worker_function,
        args,
    ) {
        Ok(v) => println!("Successfully produced messages, returned value is {:#?}!", v),
        Err(e) => println!("Error producing messages: {:?}", e),
    }
}

fn basic_sync_worker_function(
    producer: &BaseProducer,
    topic: &'static str,
    args: Vec<Arc<dyn Any + Sync + Send>>,
) -> i32 {
    let message = args[0].clone();
    let key = args[1].clone();

    let message = message.downcast_ref::<String>().unwrap();
    let key = key.downcast_ref::<String>().unwrap();

    let send_res = producer.send(BaseRecord::to(topic).payload(message).key(key));

    // Flush the producer, to ensure that all messages are sent
    // or the duration has passed.
    producer.flush(Duration::from_secs(3));

    match send_res {
        Ok(_) => println!("Successfully sent message!"),
        Err(e) => println!("Error sending message: {:?}", e),
    }

    // Drop the producer, to ensure that all messages are sent
    drop(producer);

    // Return dummy value
    1337
}
