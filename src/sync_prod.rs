use log::debug;
use rdkafka::{producer::BaseProducer, ClientConfig};
use std::sync::mpsc::channel;

use std::thread;
use std::{any::Any, boxed::Box, sync::Arc};

use crate::prod_utils::validate_produce_input;
//use prod_utils::validate_produce_input;

/// Type alias for a synchronous worker function.
/// The worker function is assumed to expect a BaseProducer, a topic name, and a vector of arguments as its parameter.
/// The arguments in the vector are type-erased, so the worker function is expected to know what to do with them.
/// The function is expected to return a value of type T.
pub type SyncWorkerFunction<T> =
    fn(&BaseProducer, &'static str, Vec<Arc<dyn Any + Sync + Send>>) -> T;

/// Executes the given worker function in parallel, using the given number of threads.
/// Sends the given arguments to the worker function, which is assumed to expect a
/// Baseproducer, a topic name, and a vector of arguments as its parameter.
/// Will create one producer instance per thread.
///
/// # Arguments
///
/// * `n_threads` - The number of threads to use for parallel execution
/// * `topic` - The topic to produce to
/// * `producer_config` - The configuration used to create the producers
/// * `worker_function` - The function to execute in parallel
/// * `worker_function_args` - The arguments to pass to the worker function, beyond the producer and topic
///
/// # Returns
///
/// * `Ok(Vec<T>)` - If the worker function returns a value, it will be returned in a vector
/// * `Err(Box<dyn Any + Send>)` - If an error occurs
pub fn produce_in_parallel<T: 'static + Clone + Send>(
    num_threads: u32,
    topic: &'static str,
    producer_config: &ClientConfig,
    worker_function: SyncWorkerFunction<T>,
    worker_function_args: Vec<Arc<dyn Any + Sync + Send>>,
) -> Result<Vec<T>, Box<dyn Any + Send>> {
    if let Err(e) = validate_produce_input(num_threads, topic, producer_config) {
        return Err(e);
    }

    let (tx, rx) = channel();

    let producer_config = Arc::new(producer_config.clone());

    let mut threads = Vec::new();

    for _ in 0..num_threads {
        let tx = tx.clone();
        let producer_config = producer_config.clone();
        let topic = topic.clone();
        let worker_function_args = worker_function_args.clone();

        threads.push(thread::spawn(move || {
            let producer: BaseProducer = producer_config.create().expect("Producer creation error");
            let result = worker_function(&producer, topic, worker_function_args);
            tx.send(result).expect("Failed to send result");
        }));
    }

    drop(tx);

    let mut results = Vec::new();

    for _ in threads {
        let result = rx.recv().expect("Failed to receive result");
        results.push(result);
    }

    Ok(results)
}

/// Type alias for a worker function that can be executed in parallel, which takes a BaseProducer,
/// a topic name, and a vector of type-erased arguments as its parameter.
/// The function is expected to return a type-erased value.
pub type SyncWorkerFunctionTypeErased =
    fn(&BaseProducer, &'static str, Vec<Arc<dyn Any + Sync + Send>>) -> Box<dyn Any + Sync + Send>;

/// Executes the given worker function in parallel, using the given number of threads.
/// Sends the given arguments to the worker function, which is assumed to expect a
/// Baseproducer, a topic name, and a vector of arguments as its parameter.
/// Will create one producer instance per thread.
///
/// # Arguments
///
/// * `n_threads` - The number of threads to use for parallel execution
/// * `topic` - The topic to produce to
/// * `producer_config` - The configuration used to create the producers
/// * `worker_function` - The function to execute in parallel
/// * `worker_function_args` - The arguments to pass to the worker function, beyond the producer and topic
///
/// # Returns
///
/// * `Ok(Some(Vec<Box<dyn Any + Send + Sync>>))` - If the worker function returns a value, it will be returned in a vector
/// * `Ok(None)` - If the worker function does not return a value
/// * `Err(Box<dyn Any + Send>)` - If an error occurs
pub fn produce_in_parallel_type_erased(
    n_threads: u32,
    topic: &'static str,
    producer_config: &ClientConfig,
    worker_function: SyncWorkerFunctionTypeErased,
    worker_function_args: Vec<Arc<dyn Any + Sync + Send>>,
) -> Result<Vec<Box<dyn Any + Send + Sync>>, Box<dyn Any + Send>> {
    if let Err(e) = validate_produce_input(n_threads, topic, producer_config) {
        return Err(e);
    }

    let mut thread_handles = Vec::new();

    // Channels for sending local thread results back to main thread
    let (tx, rx) = channel();

    for thread_id in 0..n_threads {
        debug!("Starting thread {}", thread_id);
        let tx = tx.clone();
        match producer_config.create::<BaseProducer>() {
            Ok(producer) => {

                let worker_function = Arc::new(worker_function);

                let function_args = worker_function_args.clone();

                let handle = thread::spawn(move || {
                    match tx.send(worker_function(&producer, topic, function_args)) {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!("Failed to send result from thread {}: {}", thread_id, e);
                            // If we fail to send, will block at recv() call, so just panic
                            panic!("Failed to send result from thread {}: {}", thread_id, e);
                        }
                    }
                });

                thread_handles.push(handle);
            }
            Err(e) => return Err(Box::new(e)),
        }
    }

    drop(tx);

    let mut results = Vec::new();

    log::debug!("Joining threads");
    for handle in thread_handles {
        match handle.join() {
            Ok(_) => {
                log::debug!("Thread joined");
                let local_res = rx.recv().unwrap();
                results.push(local_res);
            }
            Err(e) => return Err(e),
        }
    }
    log::debug!("Threads joined successfully");

    return Ok(results);
}

#[cfg(test)]
mod tests {

    use super::*;

    fn get_default_producer_config() -> ClientConfig {
        let mut producer_config = ClientConfig::new();
        producer_config.set("message.timeout.ms", "10");
        producer_config.set("bootstrap.servers", "localhost:9092");

        producer_config
    }

    fn test_sync_worker_function_returns_some(
        _: &BaseProducer,
        _: &'static str,
        _: Vec<Arc<dyn Any + Sync + Send>>,
    ) -> Box<dyn Any + Sync + Send> {
        Box::new(Some(1337))
    }

    fn test_sync_worker_function_returns_none(
        _: &BaseProducer,
        _: &'static str,
        _: Vec<Arc<dyn Any + Sync + Send>>,
    ) -> Box<dyn Any + Sync + Send> {
        Box::new(None::<()>)
    }

    #[test]
    fn test_produce_in_parallel_returns_err() {
        let producer_config = get_default_producer_config();

        match produce_in_parallel_type_erased(
            0,
            "test-topic",
            &producer_config,
            test_sync_worker_function_returns_none,
            Vec::new(),
        ) {
            Ok(_) => assert!(false),
            Err(_) => assert!(true),
        }

        match produce_in_parallel_type_erased(
            2,
            "",
            &producer_config,
            test_sync_worker_function_returns_none,
            Vec::new(),
        ) {
            Ok(_) => assert!(false),
            Err(_) => assert!(true),
        }

        let mut producer_config = ClientConfig::new();
        producer_config.set("message.timeout.ms", "10");

        match produce_in_parallel_type_erased(
            2,
            "test-topic",
            &producer_config,
            test_sync_worker_function_returns_none,
            Vec::new(),
        ) {
            Ok(_) => assert!(false),
            Err(_) => assert!(true),
        }

        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", "localhost:9092");

        match produce_in_parallel_type_erased(
            2,
            "test-topic",
            &producer_config,
            test_sync_worker_function_returns_none,
            Vec::new(),
        ) {
            Ok(_) => assert!(false),
            Err(_) => assert!(true),
        }
    }

    #[test]
    fn test_produce_in_parallel_returns_none() {
        let producer_config = get_default_producer_config();

        match produce_in_parallel_type_erased(
            2,
            "test-topic",
            &producer_config,
            test_sync_worker_function_returns_none,
            Vec::new(),
        ) {
            Ok(res) => {
                for val in res {
                    let val = val.downcast::<Option<()>>().unwrap();
                    assert!(val.is_none());
                }
            }
            Err(_) => assert!(false),
        }
    }

    // Should write some tests that check that the worker function returns the correct result
    #[test]
    fn test_produce_in_parallel_returns_correct_values() {
        let producer_config = get_default_producer_config();

        match produce_in_parallel_type_erased(
            2,
            "test-topic",
            &producer_config,
            test_sync_worker_function_returns_some,
            Vec::new(),
        ) {
            Ok(res) => {
                for val in res {
                    let val = val.downcast::<Option<i32>>().unwrap();
                    assert!(val.is_some());
                    let val = val.unwrap();
                    assert_eq!(val, 1337);
                }
            }
            Err(_) => assert!(false),
        }
    }
}
