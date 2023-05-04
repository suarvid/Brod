use std::{any::Any, sync::Arc};

use rdkafka::ClientConfig;

/// Returns a default producer config, setting only the given bootstrap servers and message timeout.
/// The other configuration options are set to their default values.
///
/// # Arguments
///
/// * `bootstrap_servers` - A string slice that holds the bootstrap servers to connect to.
/// * `message_timeout_ms` - A string slice that holds the message timeout in milliseconds.
///
/// # Returns
///
/// * `producer_config` - A ClientConfig object with the given bootstrap servers and message timeout.
pub fn get_default_producer_config(
    bootstrap_servers: &str,
    message_timeout_ms: &str,
) -> ClientConfig {
    let mut producer_config = ClientConfig::new();
    producer_config.set("message.timeout.ms", message_timeout_ms);
    producer_config.set("bootstrap.servers", bootstrap_servers);
    producer_config
}

/// Casts a vector of type T to a vector of type Vec<Arc<dyn Any + Send + Sync>>,
/// erasing the type information in the process.
/// Makes it easier to pass arguments to the worker function, as the conversions
/// need not be done manually.
///
/// # Arguments
///
/// * `to_add` - A vector of type T, where T implements Sync + Send + Clone + 'static
///
/// # Returns
///
/// * `return_vec` - A vector of type Arc<dyn Any + Send + Sync>
pub fn type_erase_args_vec_async<T>(to_add: Vec<T>) -> Vec<Arc<dyn Any + Send + Sync>>
where
    T: Sync + Send + Clone + 'static,
{
    let mut return_vec = vec![];
    for val in to_add {
        let val = Arc::new(val.clone());
        return_vec.push(val as Arc<dyn Any + Sync + Send>)
    }

    return_vec
}

/// Casts a vector of type T to a vector of type Vec<Arc<dyn Any + Send + Sync>>,
/// erasing the type information in the process.
/// Makes it easier to pass arguments to the worker function, as the conversions
/// need not be done manually.
///
/// # Arguments
///
/// * `to_add` - A vector of type T, where T implements Sync + Send + Clone + 'static
///
/// # Returns
///
/// * `return_vec` - A vector of type Arc<dyn Any + Send + Sync>
pub fn type_erase_args_vec_sync<T>(to_add: Vec<T>) -> Vec<Box<dyn Any + Send + Sync>>
where
    T: Sync + Send + Clone + 'static,
{
    let mut return_vec = vec![];
    for val in to_add {
        let val = Box::new(val.clone());
        return_vec.push(val as Box<dyn Any + Sync + Send>)
    }

    return_vec
}

/// Casts a single argument of type T to a single argument of type Arc<dyn Any + Send + Sync>,
/// erasing the type information in the process.
/// Makes it easier to pass arguments to the worker function, as the conversions
/// need not be done manually.
///
/// # Arguments
///
/// * `to_add` - A single argument of type T, where T implements Sync + Send + Clone + 'static
///
/// # Returns
///
/// * A single argument of type Arc<dyn Any + Send + Sync>
pub fn type_erase_single_arg_async<T>(to_add: T) -> Arc<dyn Any + Send + Sync>
where
    T: Sync + Send + Clone + 'static,
{
    Arc::new(to_add.clone()) as Arc<dyn Any + Sync + Send>
}

/// Casts a single argument of type T to a single argument of type Arc<dyn Any + Send + Sync>,
/// erasing the type information in the process.
/// Makes it easier to pass arguments to the worker function, as the conversions
/// need not be done manually.
///
/// # Arguments
///
/// * `to_add` - A single argument of type T, where T implements Sync + Send + Clone + 'static
///
/// # Returns
///
/// * A single argument of type Arc<dyn Any + Send + Sync>
pub fn type_erase_single_arg_sync<T>(to_add: T) -> Box<dyn Any + Send + Sync>
where
    T: Sync + Send + Clone + 'static,
{
    Box::new(to_add.clone()) as Box<dyn Any + Sync + Send>
}

/// Validates the given input parameters, to ensure that they are valid.
///
/// # Arguments
///
/// * `n_threads` - The number of threads to use for parallel execution
/// * `topic` - The topic to produce to
/// * `producer_config` - The configuration used to create the producers
///
/// # Returns
///
/// * Result<(), Box<dyn Any + Send>> - Returns nothing if the configuration is valid, or an error if it is not
pub fn validate_produce_input(
    n_threads: u32,
    topic: &'static str,
    producer_config: &ClientConfig,
) -> Result<(), Box<dyn Any + Send>> {
    if n_threads == 0 {
        return Err(Box::new("n_threads must be greater than 0!"));
    }

    if topic.is_empty() {
        return Err(Box::new("topic must not be empty!"));
    }

    validate_producer_config(producer_config)
}

/// Validates the given producer configuration, to ensure that it contains the
/// necessary parameters.
///
/// # Arguments
///
/// * `producer_config` - The configuration to validate
///
/// # Returns
///
/// * Result<(), Box<dyn Any + Send>> - Returns nothing if the configuration is valid, or an error if it is not
pub fn validate_producer_config(producer_config: &ClientConfig) -> Result<(), Box<dyn Any + Send>> {
    if producer_config.get("bootstrap.servers").is_none()
        || producer_config.get("bootstrap.servers").unwrap().is_empty()
    {
        return Err(Box::new("No bootstrap.servers specified!"));
    }

    if producer_config.get("message.timeout.ms").is_none()
        || producer_config
            .get("message.timeout.ms")
            .unwrap()
            .is_empty()
    {
        return Err(Box::new("No message.timeout.ms specified!"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_default_config() {
        let producer_config = get_default_producer_config("localhost:9092", "10");
        assert_eq!(
            producer_config.get("bootstrap.servers").unwrap(),
            "localhost:9092"
        );
        assert_eq!(producer_config.get("message.timeout.ms").unwrap(), "10");
    }

    #[test]
    fn test_validate_producer_config_valid() {
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", "localhost:9092");
        producer_config.set("message.timeout.ms", "10");
        assert!(validate_producer_config(&producer_config).is_ok());

        let producer_config = get_default_producer_config("localhost:9092", "10");
        assert!(validate_producer_config(&producer_config).is_ok());
    }

    #[test]
    fn test_validate_producer_config_invalid() {
        let producer_config = ClientConfig::new();
        assert!(validate_producer_config(&producer_config).is_err());
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", "localhost:9092");
        assert!(validate_producer_config(&producer_config).is_err());
        let mut producer_config = ClientConfig::new();
        producer_config.set("message.timeout.ms", "10");
        assert!(validate_producer_config(&producer_config).is_err());
    }

    #[test]
    fn test_type_erase_args_valid() {
        let to_add = vec![1, 2, 3, 4, 5];
        let return_vec = type_erase_args_vec_async(to_add);
        assert_eq!(return_vec.len(), 5);

        for i in 0..5 {
            assert_eq!(
                *return_vec[i].clone().downcast::<i32>().unwrap(),
                i as i32 + 1
            );
        }

        let to_add = vec![1, 2, 3, 4, 5];
        let return_vec = type_erase_args_vec_sync(to_add);
        assert_eq!(return_vec.len(), 5);
    }
}
