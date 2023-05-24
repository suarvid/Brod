use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{mpsc::channel, Arc},
};

use crate::prod_utils::validate_produce_input;
use rdkafka::{producer::FutureProducer, ClientConfig};

/// Type alias for a boxed future returned by an async function, containing a value of type T.
pub type DynFut<'lt, T> = Pin<Box<dyn 'lt + Send + Future<Output = T>>>;

/// Type alias for a boxed async function that returns a future of type T.
pub type AsyncWorkerFunction<T> =
    fn(FutureProducer, &'static str, Vec<Arc<dyn Any + Send + Sync>>) -> DynFut<'static, T>;

/// Type alias for a boxed async function that returns a future of type T.
/// The inner function takes a FutureProducer, a topic name, and a vector of arguments.
/// The vector of arguments should be type-erased.
#[derive(Clone, Copy)]
pub struct AsyncFunctionWrapper<T> {
    inner: AsyncWorkerFunction<T>,
}

impl<T> AsyncFunctionWrapper<T> {
    /// Creates a new AsyncFunctionWrapper from the given function.
    /// The inner function takes a FutureProducer, a topic name, and a vector of type-erased arguments.
    ///
    /// # Arguments
    ///
    /// * `inner` - The inner function to wrap
    ///
    /// # Returns
    ///
    /// * `AsyncFunctionWrapper<T>` - The wrapped function
    pub fn new(inner: AsyncWorkerFunction<T>) -> Self {
        Self { inner }
    }

    /// Runs the wrapped function, passing the given producer, topic, and arguments.
    ///
    /// # Arguments
    ///
    /// * `producer` - The producer to use
    /// * `topic` - The topic to produce to
    /// * `args` - The arguments to pass to the wrapped function
    ///
    /// # Returns
    ///
    /// * `DynFut<'static, T>` - A boxed future containing the result of the wrapped function
    pub fn run(
        &self,
        producer: FutureProducer,
        topic: &'static str,
        args: Vec<Arc<dyn Any + Send + Sync>>,
    ) -> DynFut<'static, T> {
        (self.inner)(producer, topic, args)
    }
}

/// Executes the given worker function in parallel, using the given number of threads.
/// Sends the given arguments to the worker function, which is assumed to expect a
/// FutureProducer, a topic name, and a vector of arguments as its parameters.
///
/// # Arguments
///
/// * `num_threads` - The number of threads to use for parallel execution
/// * `topic` - The topic to produce to
/// * `producer_config` - The configuration used to create the producers
/// * `func_wrapper` - A wrapper for the worker function
/// * `args` - The arguments to pass to the worker function
///
/// # Returns
///
/// * `Result<Vec<T>, Box<dyn Any + Send>>` - A vector containing the results of the worker function, or an error if one occurred
pub async fn produce_in_parallel<T: 'static + Clone + Send>(
    num_threads: u32,
    topic: &'static str,
    producer_config: &ClientConfig,
    func_wrapper: AsyncFunctionWrapper<T>,
    args: Vec<Arc<dyn Any + Send + Sync>>,
) -> Result<Vec<T>, Box<dyn Any + Send>> {
    if let Err(e) = validate_produce_input(num_threads, topic, producer_config) {
        return Err(e);
    }

    let mut thread_handles = Vec::new();
    let (tx, rx) = channel();

    for thread_id in 0..num_threads {
        log::debug!("Spawning thread {}", thread_id);
        let tx = tx.clone();
        match producer_config.create::<FutureProducer>() {
            Ok(producer) => {
                let topic = topic.clone();
                let worker_function = Arc::new(func_wrapper.clone());
                let worker_function_args = args.clone();
                let handle = tokio::spawn(async move {
                    let local_res = worker_function
                        .run(producer, topic, worker_function_args)
                        .await;
                    match tx.send(local_res) {
                        Ok(_) => log::debug!("Sent result from thread {}", thread_id),
                        Err(e) => {
                            log::error!("Error sending result from thread {}: {}", thread_id, e);
                            panic!("Error sending result from thread {}: {}", thread_id, e);
                        }
                    }
                });
                thread_handles.push(handle);
            }
            Err(e) => {
                log::error!("Error creating producer: {}", e);
                return Err(Box::new(e));
            }
        }
    }

    drop(tx);

    let mut results = Vec::new();

    for handle in thread_handles {
        match handle.await {
            Ok(_) => {
                log::debug!("Thread joined");
                match rx.recv() {
                    Ok(local_res) => results.push(local_res),
                    Err(e) => log::error!("Error receiving result from thread: {}", e),
                }
            }
            Err(e) => log::error!("Error joining thread: {}", e),
        };
    }

    Ok(results)
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use rdkafka::producer::FutureRecord;

    use super::*;
    use crate::prod_utils;

    // Macro which makes testing async functions easier
    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    async fn test_async_fn_returns_i32(
        _: FutureProducer,
        _: &'static str,
        _: Vec<Arc<dyn Any + Send + Sync>>,
    ) -> i32 {
        1337
    }

    #[test]
    fn test_produce_in_parallel_returns_correct_values() {
        // Dummy values, don't really matter
        let producer_config = prod_utils::get_default_producer_config("localhost:9092", "10");
        let topic = "test-topic";
        let args = Vec::new();
        let num_threads = 4;

        let func_wrapper = AsyncFunctionWrapper::new(|future_producer, topic, args| {
            Box::pin(test_async_fn_returns_i32(future_producer, topic, args))
        });

        let t = aw!(produce_in_parallel(
            num_threads,
            topic,
            &producer_config,
            func_wrapper,
            args
        ));
        assert!(t.is_ok());

        let results = t.unwrap();
        assert_eq!(results.len(), num_threads as usize);
        for res in results {
            assert_eq!(res, 1337);
        }
    }

    #[test]
    fn test_produce_in_parallel_returns_error_on_invalid_num_threads() {
        // Dummy values, don't really matter
        let producer_config = prod_utils::get_default_producer_config("localhost:9092", "10");
        let topic = "test-topic";
        let args = Vec::new();
        let num_threads = 0;

        let func_wrapper = AsyncFunctionWrapper::new(|future_producer, topic, args| {
            Box::pin(test_async_fn_returns_i32(future_producer, topic, args))
        });

        let t = aw!(produce_in_parallel(
            num_threads,
            topic,
            &producer_config,
            func_wrapper,
            args
        ));
        assert!(t.is_err());
    }

    #[test]
    fn test_produce_in_parallel_returns_error_on_invalid_topic() {
        // Dummy values, don't really matter
        let producer_config = prod_utils::get_default_producer_config("localhost:9092", "10");
        let topic = "";
        let args = Vec::new();
        let num_threads = 4;

        let func_wrapper = AsyncFunctionWrapper::new(|future_producer, topic, args| {
            Box::pin(test_async_fn_returns_i32(future_producer, topic, args))
        });

        let t = aw!(produce_in_parallel(
            num_threads,
            topic,
            &producer_config,
            func_wrapper,
            args
        ));
        assert!(t.is_err());
    }

    #[test]
    fn test_produce_in_parallel_returns_error_on_invalid_producer_config() {
        // Dummy values, don't really matter
        let producer_config = ClientConfig::new();
        let topic = "test-topic";
        let args = Vec::new();
        let num_threads = 4;

        let func_wrapper = AsyncFunctionWrapper::new(|future_producer, topic, args| {
            Box::pin(test_async_fn_returns_i32(future_producer, topic, args))
        });

        let t = aw!(produce_in_parallel(
            num_threads,
            topic,
            &producer_config,
            func_wrapper,
            args
        ));
        assert!(t.is_err());
    }

    async fn checks_message_response(
        producer: FutureProducer,
        topic: &'static str,
        _: Vec<Arc<dyn Any + Sync + Send>>,
    ) -> Result<(i32, i64), (rdkafka::error::KafkaError, rdkafka::message::OwnedMessage)> {
        let i = 0;
        let delivery_status = producer
            .send(
                FutureRecord::to(topic)
                    .payload(&format!("Message {}", i))
                    .key(&format!("Key {}", i)),
                Duration::from_secs(5),
            )
            .await;

        if let Err(ref e) = delivery_status {
            log::error!("Error sending message: {:?}", e);
        }

        delivery_status
    }

    /// This test is a bit more involved than the others, as it actually sends messages to Kafka
    /// and checks that they are received correctly.
    /// It also checks that the number of messages sent is correct.
    /// This test is a bit flaky, as it relies on the Kafka broker being up and running.
    /// It also relies on the topic being created beforehand.
    ///
    /// # Required setup
    ///
    /// * Kafka broker running on localhost:9092
    /// * Topic "test-topic" created on the broker
    #[test]
    fn test_produce_in_parallel_sends_messages() {
        pretty_env_logger::init();

        let producer_config = prod_utils::get_default_producer_config("localhost:9092", "5000");
        //let future_producer: FutureProducer = producer_config.create().unwrap();
        let topic = "test-topic";
        let args = Vec::new();
        let num_threads = 4;

        let func_wrapper = AsyncFunctionWrapper::new(|future_producer, topic, args| {
            Box::pin(checks_message_response(future_producer, topic, args))
        });

        let t = aw!(produce_in_parallel(
            num_threads,
            topic,
            &producer_config,
            func_wrapper,
            args
        ));
        assert!(t.is_ok());

        let results = t.unwrap();
        assert_eq!(results.len(), num_threads as usize);
        for res in results {
            assert!(res.is_ok());
        }
    }
}
