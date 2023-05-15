// Maybe a struct which holds several producer instances, exposes
// associated functions for sending messages, which alternates the
// producer instance used between each call, rotating between the instances?
// Something like ProducerPool?
//

use futures::lock;
use num_cpus;
use rdkafka::{
    error::KafkaError,
    message::{OwnedMessage, ToBytes},
    producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord},
    ClientConfig,
};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct SyncProducerPool {
    n_producers: usize,
    current_index: Arc<Mutex<usize>>,
    producers: Vec<BaseProducer>,
}

impl SyncProducerPool {
    pub fn new(n_producers: usize, config: &ClientConfig) -> Result<Self, KafkaError> {
        let mut producers = Vec::new();

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
        *index += 1;
        *index = *index % self.n_producers;

        drop(index);

        let res = p.send(BaseRecord::to(topic).payload(&payload).key(&key));
        if let Err((e, _)) = res {
            return Err(e);
        }

        Ok(())
    }
}

pub struct AsyncProducerPool {
    n_producers: usize,
    current_index: Arc<lock::Mutex<usize>>,
    //current_index: Arc<Mutex<usize>>,
    producers: Vec<FutureProducer>,
}

impl AsyncProducerPool {
    pub fn new(n_producers: usize, config: &ClientConfig) -> Result<Self, KafkaError> {
        let mut producers = Vec::new();
        for _ in 0..n_producers {
            match config.create() {
                Ok(producer) => producers.push(producer),
                Err(e) => return Err(e),
            }
        }

        Ok(Self {
            n_producers: n_producers,
            current_index: Arc::new(lock::Mutex::new(0)),
            producers: producers,
        })
    }

    pub async fn publish(
        &self,
        topic: &str,
        key: String,
        payload: String,
    ) -> Result<(), KafkaError> {
        let mut index = self.current_index.lock().await;
        //.expect("Error obtaining index lock in ProducerPool!");

        let p = self.producers.get(*index).expect("Failed to get producer!");

        *index += 1;
        *index = *index % self.n_producers;
        // Drop index in order to unlock the mutex
        drop(index);

        let res = p
            .send(
                FutureRecord::to(topic).key(&key).payload(&payload),
                Duration::from_secs(5),
            )
            .await;

        if let Err((e, _)) = res {
            return Err(e);
        }

        Ok(())
    }
}
