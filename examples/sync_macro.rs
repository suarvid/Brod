use brod::prod_utils;
use brod::produce_parallel;
use rdkafka::producer::BaseProducer;
use rdkafka::producer::BaseRecord;

fn add(p: BaseProducer, a: i32, b: i32) -> i32 {
    a + b
}

fn publish_some_stuff(
    prod: BaseProducer,
    topic: &str,
    times_to_publish: i32,
    key: String,
    message: String,
) -> u32 {
    for i in 0..times_to_publish {
        match prod.send(BaseRecord::to(topic).key(&key).payload(&message)) {
            Ok(_) => println!("Successfully sent message {}", i),
            Err(_) => println!("Failed to send message"),
        }
    }

    1337
}

fn publish_some_other_stuff(
    prod: BaseProducer,
    topic: &str,
    times_to_publish: i32,
    key: String,
    message: String,
) -> usize {
    for i in 0..times_to_publish {
        match prod.send(BaseRecord::to(topic).key(&key).payload(&message)) {
            Ok(_) => println!("Successfully sent message {}", i),
            Err(_) => println!("Failed to send message"),
        }
    }

    1337
}

fn main() {
    let num_threads = 4;
    let topic = "test-topic";

    let producer_config = &prod_utils::get_default_producer_config("localhost:9092", "100");

    //spawn_threads_with_args!(num_threads; producer_config; add, 1, 2; 3, 4);
    let results = produce_parallel!(num_threads; producer_config; publish_some_stuff,
        topic, 5, String::from("Henlo"), String::from("Henlo");
        topic, 100, String::from("Baja"), String::from("Maja"));

    for r in results {
        println!("r: {:#?}", r);
    }

    let results = produce_parallel!(num_threads; producer_config; publish_some_other_stuff,
        topic, 5, String::from("A"), String::from("B"));

    for r in results {
        println!("r: {:#?}", r);
    }
}
