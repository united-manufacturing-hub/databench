use crate::receiver::Receiver;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::sleep;

pub(crate) struct KafkaReceiver {
    receiving: Arc<AtomicBool>,
    brokers: Vec<String>,
    topic: String,
    hashes: Arc<RwLock<VecDeque<String>>>,
    received_message_cnt: Arc<AtomicU64>,
}

impl Receiver for KafkaReceiver {
    fn new(brokers: Vec<String>, topic: String) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            brokers,
            receiving: Arc::new(AtomicBool::new(false)),
            topic,
            hashes: Arc::new(RwLock::new(VecDeque::new())),
            received_message_cnt: Arc::new(AtomicU64::new(0)),
        })
    }

    fn begin(&mut self) -> anyhow::Result<()> {
        // If already receiving, return
        if self.receiving.swap(true, Ordering::Relaxed) {
            return Ok(());
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
        let received_messages_counter = self.received_message_cnt.clone();

        let topic = self.topic.clone();
        thread::spawn(move || {
            let topics = vec![topic.as_str()];
            let mut hasher = blake3::Hasher::new();
            let mut thread_hashes = VecDeque::new();

            #[allow(clippy::expect_used)]
            consumer
                .subscribe(&topics)
                .expect("Can't subscribe to specified topics");
            while receiving.load(Ordering::Relaxed) {
                match consumer.poll(Timeout::After(std::time::Duration::from_millis(1000))) {
                    None => {
                        sleep(std::time::Duration::from_millis(10));
                        continue;
                    }
                    Some(kafkaresult) => match kafkaresult {
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
                            received_messages_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => {
                            eprintln!("Error while receiving from kafka: {}", error)
                        }
                    },
                };
                if received_messages_counter.load(Ordering::Relaxed) % 10000 == 0 {
                    println!("Received {} messages", received_messages_counter.load(Ordering::Relaxed));
                    let hashlock = hashes.write();
                    #[allow(clippy::expect_used)]
                    let mut hashes = hashlock.expect("Failed to get write lock");
                    for hash in thread_hashes.drain(..) {
                        hashes.push_back(hash);
                    }
                    thread_hashes.clear();
                }
            }

            println!("Received {} messages", received_messages_counter.load(Ordering::Relaxed));
            let hashlock = hashes.write();
            #[allow(clippy::expect_used)]
            let mut hashes = hashlock.expect("Failed to get write lock");
            for hash in thread_hashes.drain(..) {
                hashes.push_back(hash);
            }
            thread_hashes.clear();
        });

        Ok(())
    }

    fn end(&mut self) {
        self.receiving.swap(false, Ordering::Relaxed);
    }

    fn get_received_messages_hashes(&self) -> VecDeque<String> {
        #[allow(clippy::expect_used)]
        self.hashes.read().expect("Failed to get read lock").clone()
    }

    fn get_received_messages(&self) -> u64 {
        self.received_message_cnt.load(Ordering::Relaxed)
    }
}
