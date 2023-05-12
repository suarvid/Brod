// Maybe a struct which holds several producer instances, exposes
// associated functions for sending messages, which alternates the
// producer instance used between each call, rotating between the instances?
// Something like ProducerPool?
//

use num_cpus;
use rdkafka::{
    error::KafkaError,
    message::ToBytes,
    producer::{BaseProducer, BaseRecord},
    ClientConfig,
};
use std::sync::{Arc, Mutex};

pub struct SyncProducerPool {
    n_producers: usize,
    current_index: Arc<Mutex<usize>>,
    producers: Vec<BaseProducer>,
}

impl SyncProducerPool {
    pub fn new_with_n_producers(
        n_producers: usize,
        config: &ClientConfig,
    ) -> Result<Self, KafkaError> {
        let mut producers = vec![];

        for _ in 0..n_producers {
            match config.create() {
                Ok(producer) => producers.push(producer),
                Err(e) => return Err(e),
            }
        }

        Ok(Self {
            n_producers: n_producers,
            current_index: Arc::new(Mutex::new(0)),
            producers: producers,
        })
    }

    pub fn new(config: &ClientConfig) -> Result<Self, KafkaError> {
        let n_cpus = num_cpus::get();
        Self::new_with_n_producers(n_cpus, config)
    }

    pub fn publish<K, P>(&self, topic: &str, key: K, payload: P) -> Result<(), KafkaError>
    where
        K: ToBytes,
        P: ToBytes,
    {
        let mut index = self
            .current_index
            .lock()
            .expect("Failed to obtain index lock in ProducerPool!");

        let p = self
            .producers
            .get(*index)
            .expect("Panicked when trying to access producer");

        match p.send(BaseRecord::to(topic).payload(&payload).key(&key)) {
            Ok(_) => {
                *index += 1;
                *index = *index % self.n_producers;
            }
            Err((e, _)) => return Err(e),
        }

        Ok(())
    }
}
