use std::collections::VecDeque;
use anyhow::Result;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::ClientConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::error::RDKafkaErrorCode::OperationTimedOut;
use rdkafka::types::RDKafkaErrorCode::QueueFull;

use crate::generator::Generator;
use crate::sender::Sender;
use rdkafka::util::Timeout;

struct KafkaSender {
    sending: Arc<AtomicBool>,
    brokers: Vec<String>,
    hashes: Arc<RwLock<Vec<String>>>,
}

impl Sender for KafkaSender {
    fn new(brokers: Vec<String>) -> Result<Self> {
        Ok(Self {
            brokers,
            sending: Arc::new(AtomicBool::new(false)),
            hashes: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn begin<T: Generator>(
        &mut self,
        split_point: usize,
        topics: usize,
    ) -> Result<()> {
        // If already sending, return
        if self.sending.swap(true, Ordering::Relaxed) {
            return Ok(());
        }


        // Create a new thread, which sends up to _messages_per_second messages per second
        let sending = self.sending.clone();

        // Init kafka producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.join(","))
            .set("message.timeout.ms", "50000")
            .set("batch.num.messages", "10000")
            .create()?;

        let hashes = self.hashes.clone();

        thread::spawn(move || {
            #[allow(clippy::expect_used)]
            let generator = T::new(split_point, topics).expect("Failed to create generator");
            let mut produced = 0;
            let mut hasher = blake3::Hasher::new();
            let mut thread_hashes = VecDeque::new();
            let now = std::time::Instant::now();
            while sending.load(Ordering::Relaxed) {

                let message = generator.get_message();
                match message {
                    Ok(msg) => {
                        hasher.reset();

                        // Re-assemble original topic by concatenating topic and key with a dot
                        let topic = format!("{}.{}",msg.topic, msg.key);

                        hasher.update(topic.as_bytes());
                        hasher.update(&msg.value);

                        let raw_hash = hasher.finalize();
                        let hash = hex::encode(raw_hash.as_bytes());

                        thread_hashes.push_back(hash.clone());

                        // Send the message
                        match producer
                            .send(BaseRecord::to(&msg.topic).payload(&msg.value).key(&msg.key))
                        {
                            Ok(_) => {
                                produced += 1;
                            }
                            Err(produce_feedback) => {

                                match produce_feedback.0 {
                                    KafkaError::MessageProduction(produce_rdkafka_error) => {
                                        if produce_rdkafka_error == QueueFull {
                                            flush(&producer,0);
                                            continue;
                                        }
                                    }
                                    _ => {
                                        eprintln!("Error sending message: {:?}", produce_feedback);
                                        continue;
                                    }
                                }


                                continue;
                            }
                        };
                    }
                    Err(err) => {
                        eprintln!("Error generating message: {:?}", err);
                        continue;
                    }
                }

                if produced % 10000 == 0 {
                    let served = producer.poll(Timeout::After(std::time::Duration::from_millis(100)));
                    println!("Produced {} ({}/s) messages | Served: {}", produced, produced as f64 / now.elapsed().as_secs_f64(), served);

                    // Drop the lock as soon as possible
                    {
                        let hashlock = hashes.write();
                        #[allow(clippy::expect_used)]
                            let mut hashes = hashlock.expect("Failed to get write lock");
                        for hash in thread_hashes.drain(..) {
                            hashes.push(hash);
                        }
                        thread_hashes.clear();
                    }

                }
            }
            producer.poll(Timeout::After(std::time::Duration::from_millis(100)));
        });

        Ok(())
    }

    fn end(&mut self) {
        self.sending.store(false, Ordering::Relaxed);
    }

    fn get_send_message_hashes(&self) -> Vec<String> {
        #[allow(clippy::expect_used)]
        self.hashes.read().expect("Failed to get read lock").clone()
    }
}

fn flush(producer: &BaseProducer, mut depth: i32) {
    if depth > 10 {
        depth = 10;
    }
    eprintln!("Queue full, waiting");
    match producer.flush(Timeout::After(std::time::Duration::from_millis((1000 * (depth + 1)) as u64))) {
        Ok(_) => {}
        Err(flush_error) => {
            match flush_error {
                KafkaError::Flush(flush_rd_kafka_error) => {
                    if flush_rd_kafka_error == OperationTimedOut {
                        flush(producer, depth + 1);
                    }
                }
                _ => {
                    eprintln!("Error flushing producer: {:?}", flush_error);

                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::chernobyl::Chernobyl;

    #[test]
    #[allow(clippy::expect_used)]
    fn test_kafka_sender() {
        let mut sender = KafkaSender::new(vec![
            "10.99.112.33:31092".to_string(),
            "10.99.112.34:31092".to_string(),
            "10.99.112.35:31092".to_string(),
        ])
        .expect("Failed to create sender");
        let seconds = 60;

        let now = std::time::Instant::now();

        sender
            .begin::<Chernobyl>(3, 100)
            .expect("Failed to begin sending");
        thread::sleep(std::time::Duration::from_secs(seconds));
        sender.end();
        let elapsed = now.elapsed();

        // Wait for kafka to catch up
        thread::sleep(std::time::Duration::from_secs(5));

        // Get hashes
        let hashes = sender.get_send_message_hashes();
        assert!(hashes.len() > 0);

        let msg_per_sec = hashes.len() as f64 / elapsed.as_secs_f64();

        println!("Sent {} ({}/s) messages in {:?}", hashes.len(), msg_per_sec , elapsed);
    }
}
