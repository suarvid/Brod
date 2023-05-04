use std::{any::Any, sync::Arc, time::Duration};

use brod::{prod_utils::{self, type_erase_single_arg_sync}, sync_prod::produce_in_parallel_type_erased};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

fn main() {
    // This is a basic example showing how to use the type-erased sync producer.

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
    // Furthermore, if we want to send an argument via the producer, the type of the
    // argument needs to implement the ToBytes trait.

    // In this example, we are sending a String as the message and a String as the key.
    args.push(Arc::new(String::from("message")) as Arc<dyn Any + Sync + Send>);
    args.push(Arc::new(String::from("key")) as Arc<dyn Any + Sync + Send>);

    match produce_in_parallel_type_erased(
        num_threads,
        topic,
        &producer_config,
        sync_worker_function_returns_type_erased_vec,
        args,
    ) {
        Ok(v) => println!(
            "Successfully produced messages, returned value is {:#?}!",
            v
        ),
        Err(e) => println!("Error producing messages: {:?}", e),
    }
}

fn sync_worker_function_returns_type_erased_vec(
    producer: &BaseProducer,
    topic: &'static str,
    args: Vec<Arc<dyn Any + Sync + Send>>,
) -> Box<dyn Any + Sync + Send> {


    let message = args[0].clone();
    let message = message.downcast_ref::<String>().unwrap();

    let key = args[1].clone();
    let key = key.downcast_ref::<String>().unwrap();

    let send_res = producer.send(BaseRecord::to(topic).payload(message).key(key));

    producer.flush(Duration::from_secs(3));

    match send_res {
        Ok(_) => println!("Successfully sent message!"),
        Err(e) => println!("Error sending message: {:?}", e),
    }

    // Can now drop the producer if we want to, depending on the use case.
    drop(producer);

    // The function used with the type-erased produce in parallel function
    // needs to return a Box<dyn Any + Sync + Send> object, i.e. a 
    // type-erased object that implements the Any, Sync and Send traits.

    // As such, it may return a vector. In this case, it is a vector of
    // type-erased objects, i.e. a vector of Box<dyn Any + Sync + Send> objects.
    let mut ret_vec: Vec<Box<dyn Any + Sync + Send>> = Vec::new();

    ret_vec.push(Box::new(String::from("Hello from the worker function!")) as Box<dyn Any + Sync + Send>);
    ret_vec.push(Box::new(1) as Box<dyn Any + Sync + Send>);

    // We can also use the type_erase_single_arg_sync function to type-erase
    // a single argument before adding it to the vector.
    ret_vec.push(type_erase_single_arg_sync(1.0));


    Box::new(ret_vec) as Box<dyn Any + Sync + Send>
}
