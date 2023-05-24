use brod::prod_utils;
use brod::spawn_threads_with_args;
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
) {
    for i in 0..times_to_publish {
        match prod.send(BaseRecord::to(topic).key(&key).payload(&message)) {
            Ok(_) => println!("Successfully sent message {}", i),
            Err(_) => println!("Failed to send message"),
        }
    }
}

fn main() {
    let num_threads = 4;
    let topic = "test-topic";

    let producer_config = &prod_utils::get_default_producer_config("localhost:9092", "100");

    //spawn_threads_with_args!(num_threads; producer_config; add, 1, 2; 3, 4);
    spawn_threads_with_args!(num_threads; producer_config; publish_some_stuff,
        topic, 5, String::from("Henlo"), String::from("Henlo");
        topic, 100, String::from("Baja"), String::from("Maja"));
}
