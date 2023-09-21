use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use rdkafka::{ClientConfig, Message};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use crate::receiver::Receiver;

struct KafkaReceiver {
    receiving: Arc<AtomicBool>,
    brokers: Vec<String>,
    topic: String,
    hashes: Arc<RwLock<Vec<String>>>,
}

impl Receiver for KafkaReceiver{
    fn new(brokers: Vec<String>, topic: String) -> anyhow::Result<Self> where Self: Sized {
        Ok(Self{
            brokers,
            receiving: Arc::new(AtomicBool::new(false)),
            topic,
            hashes: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn begin(&mut self) -> anyhow::Result<()> {
        // If already receiving, return
        if self.receiving.swap(true, Ordering::Relaxed) {
            return Ok(())
        }

        let receiving = self.receiving.clone();

        // Init kafka consumer
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.join(","))
            .set("group.id", "databench")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()?;

        let hashes = self.hashes.clone();

        let topic = self.topic.clone();
        thread::spawn(move || {
            let topics = vec![topic.as_str()];
            let mut hasher = blake3::Hasher::new();
            let mut thread_hashes = VecDeque::new();
            let mut received = 0;

            #[allow(clippy::expect_used)]
            consumer.subscribe(&topics).expect("Can't subscribe to specified topics");
            while receiving.load(Ordering::Relaxed) {
                match consumer.poll(Timeout::After(std::time::Duration::from_millis(1000))) {
                    None => {
                        continue;
                    }
                    Some(kafkaresult) => {
                        match kafkaresult {
                            Ok(message) => {
                                hasher.reset();
                                let key = message.key().unwrap_or_else(|| "".as_bytes());
                                let key_string = String::from_utf8_lossy(key).to_string();
                                let topic = format!("{}.{}", message.topic(), key_string);

                                hasher.update(topic.as_bytes());
                                hasher.update(message.payload().unwrap_or_else(|| "".as_bytes()));

                                let raw_hash = hasher.finalize();
                                let hash = hex::encode(raw_hash.as_bytes());

                                thread_hashes.push_back(hash);
                                received += 1;
                            }
                            Err(error) => {
                                eprintln!("Error while receiving from kafka: {}", error)
                            }
                        }
                    }
                };
                if received % 10000 == 0{
                    let hashlock = hashes.write();
                    #[allow(clippy::expect_used)]
                    let mut hashes = hashlock.expect("Failed to get write lock");
                    for hash in thread_hashes.drain(..) {
                        hashes.push(hash);
                    }
                    thread_hashes.clear();
                }
            }


            let hashlock = hashes.write();
            #[allow(clippy::expect_used)]
                let mut hashes = hashlock.expect("Failed to get write lock");
            for hash in thread_hashes.drain(..) {
                hashes.push(hash);
            }
            thread_hashes.clear();

        });

        Ok(())
    }

    fn end(&mut self) {
        self.receiving.swap(false, Ordering::Relaxed);
    }

    fn get_send_message_hashes(&self) -> Vec<String> {
        #[allow(clippy::expect_used)]
        self.hashes.read().expect("Failed to get read lock").clone()
    }
}
