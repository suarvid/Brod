#[macro_export]
macro_rules! spawn_threads_with_args {
    ($num_threads:expr; $prod_config:expr; $func:expr, $($($args:expr),*);*) => {
        {
            use rdkafka::ClientConfig;
            use rdkafka::producer::BaseProducer;
            use std::sync::mpsc::channel;
            use std::sync::Arc;
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
                        let producer: BaseProducer = prod_config.create().expect("Producer creation error");
                        let result = $func(producer, $($args),*);
                        println!("Thread {:?} result {:?}", std::thread::current().id(), result);
                        tx.send(result).expect("Failed to send result!");
                    }));

                }

                drop(tx);

                let mut results = Vec::new();

                for handle in handles {
                    let res = rx.recv().expect("Failed to receive result!");
                    results.push(res);
                    handle.join().unwrap();
                }


            )*

        }

    };
}
