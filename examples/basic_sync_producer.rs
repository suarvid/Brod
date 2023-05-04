use std::{any::Any, sync::Arc, time::Duration};

use kafkaesque::prod_utils::{self, type_erase_single_arg_async};
use kafkaesque::sync_prod::produce_in_parallel;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

fn main() {

    // This is a basic example showing how to use the generic sync producer.

    let num_threads = 4;
    let topic = "test-topic";

    // Default producer config, with bootstrap servers set to localhost:9092
    // and message timeout set to 10ms.
    let producer_config = prod_utils::get_default_producer_config("localhost:9092", "10");

    let mut args = Vec::new();
    // Due to the way that the type erasure works, we need to pass the arguments
    // as Arc<dyn Any + Sync + Send> objects.
    // This allows us to pass any type of arguments to the worker function.
    // However, the worker function needs to know what type of arguments it expects.
    // Furthermore, if we want to send an argument, the type of the argument needs to
    // implement the ToBytes trait.
    args.push(Arc::new(String::from("message")) as Arc<dyn Any + Sync + Send>); 

    // Can also use the type_erase_single_arg_async function to create the arguments.
    // needs to be async when passing arguments to worker functions, but the sync
    // variants may be used when returning values from sync worker functions.
    args.push(type_erase_single_arg_async(String::from("key")));

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
    let message = message.downcast_ref::<String>().unwrap();

    let key = args[1].clone();
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
