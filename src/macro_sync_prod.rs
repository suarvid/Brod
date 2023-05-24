#[macro_export]
/// Macro for executing a worker function, which is expected to produce messages to Kafka, in
/// parallel.
///
/// # Arguments
///
/// * `num_threads` - the number of threads to use for execution; an expression
/// * `prod_config` - the config object used to create producers; an expression
/// * `func` - a function pointer to the worker function; an expression
/// * `args` - zero or more repeated expressions, internally separated by a comma, representing the
/// arguments to be passed to the worker function. Separate calls to the worker function can be
/// designated by using a semicolon (;) to (externally) separate the arguments used in the
/// respective calls.
macro_rules! produce_parallel {
    ($num_threads:expr; $prod_config:expr; $func:expr, $($($args:expr),*);*) => {
        {
            use rdkafka::ClientConfig;
            use rdkafka::producer::BaseProducer;
            use std::sync::mpsc::channel;
            use std::sync::Arc;
            let mut results = Vec::new();
            $(
                let mut handles = Vec::new();
                let prod_config: &ClientConfig = $prod_config;
                let n_threads: usize = $num_threads;

                let (tx, rx) = channel();

                let prod_config = Arc::new(prod_config.clone());

                for _ in 0..n_threads {
                    let tx = tx.clone();
                    let prod_config = prod_config.clone();
                    handles.push(std::thread::spawn(move || {
                        match prod_config.create::<BaseProducer>() {
                            Ok(producer) => {
                                let result = $func(producer, $($args),*);
                                let send_res = tx.send(result);
                                if let Err(send_err) = send_res {
                                    println!("ERROR: Failed to send function result: {:#?}", send_res);
                                }
                            },
                            Err(e) => println!("ERROR: Failed to create producer instance: {:#?}", e),
                        }
                    }));
                }

                drop(tx);

                for handle in handles {
                    match rx.recv() {
                        Ok(res) => {
                            results.push(res);
                            handle.join().unwrap();
                        }
                        Err(e) => {
                            println!("ERROR: Failed to receive function result: {:#?}", e);
                            println!("WARNING: Attempting to join thread handle without receving result.");
                            handle.join().unwrap();
                        }
                    }
                }


            )*

            results
        }

    };
}
