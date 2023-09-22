use anyhow::Result;
use rdkafka::error::KafkaError;
use rdkafka::error::RDKafkaErrorCode::OperationTimedOut;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::types::RDKafkaErrorCode::QueueFull;
use rdkafka::ClientConfig;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;

use crate::generator::Generator;
use crate::sender::Sender;
use rdkafka::util::Timeout;

pub struct KafkaSender {
    sending: Arc<AtomicBool>,
    brokers: Vec<String>,
    hashes: Arc<RwLock<VecDeque<String>>>,
    send_message_cnt: Arc<AtomicU64>,
}

impl Sender for KafkaSender {
    fn new(brokers: Vec<String>) -> Result<Self> {
        Ok(Self {
            brokers,
            sending: Arc::new(AtomicBool::new(false)),
            hashes: Arc::new(RwLock::new(VecDeque::new())),
            send_message_cnt: Arc::new(AtomicU64::new(0)),
        })
    }

    fn begin<T: Generator>(&mut self, split_point: usize, topics: usize) -> Result<()> {
        // If already sending, return
        if self.sending.swap(true, Ordering::Relaxed) {
            return Ok(());
        }

        let sending = self.sending.clone();

        // Init kafka producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.join(","))
            .set("message.timeout.ms", "50000")
            .set("batch.num.messages", "10000")
            .create()?;

        let hashes = self.hashes.clone();
        let sent_messages_counter = self.send_message_cnt.clone();

        thread::spawn(move || {
            #[allow(clippy::expect_used)]
            let generator = T::new(split_point, topics).expect("Failed to create generator");
            let mut hasher = blake3::Hasher::new();
            let mut thread_hashes = VecDeque::new();
            let now = std::time::Instant::now();
            while sending.load(Ordering::Relaxed) {
                let message = generator.get_message();
                match message {
                    Ok(msg) => {
                        hasher.reset();

                        // Re-assemble original topic by concatenating topic and key with a dot
                        let topic = format!("{}.{}", msg.topic, msg.key);

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
                                sent_messages_counter.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(produce_feedback) => {
                                match produce_feedback.0 {
                                    KafkaError::MessageProduction(produce_rdkafka_error) => {
                                        if produce_rdkafka_error == QueueFull {
                                            flush(&producer, 0);
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

                if sent_messages_counter.load(Ordering::Relaxed) % 10000 == 0 {
                    match producer.flush(Timeout::After(std::time::Duration::from_millis(1000))) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Error flushing producer: {:?}", e);
                        }
                    };
                    println!(
                        "Produced {} ({}/s) messages",
                        sent_messages_counter.load(Ordering::Relaxed),
                        sent_messages_counter.load(Ordering::Relaxed) as f64
                            / now.elapsed().as_secs_f64()
                    );

                    // Drop the lock as soon as possible
                    let hashlock = hashes.write();
                    #[allow(clippy::expect_used)]
                    let mut hashes = hashlock.expect("Failed to get write lock");
                    for hash in thread_hashes.drain(..) {
                        hashes.push_back(hash);
                    }
                    thread_hashes.clear();
                }
            }
            match producer.flush(Timeout::After(std::time::Duration::from_millis(10000))) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error flushing producer: {:?}", e)
                }
            };

            // Drop the lock as soon as possible
            {
                let hashlock = hashes.write();
                #[allow(clippy::expect_used)]
                let mut hashes = hashlock.expect("Failed to get write lock");
                for hash in thread_hashes.drain(..) {
                    hashes.push_back(hash);
                }
                thread_hashes.clear();
            }
        });

        Ok(())
    }

    fn end(&mut self) {
        self.sending.store(false, Ordering::Relaxed);
    }

    fn get_sent_message_hashes(&self) -> VecDeque<String> {
        #[allow(clippy::expect_used)]
        self.hashes.read().expect("Failed to get read lock").clone()
    }

    fn get_sent_messages(&self) -> u64 {
        self.send_message_cnt.load(Ordering::Relaxed)
    }
}

fn flush(producer: &BaseProducer, mut depth: i32) {
    if depth > 10 {
        depth = 10;
    }
    eprintln!("Queue full, waiting");
    match producer.flush(Timeout::After(std::time::Duration::from_millis(
        (1000 * (depth + 1)) as u64,
    ))) {
        Ok(_) => {}
        Err(flush_error) => match flush_error {
            KafkaError::Flush(flush_rd_kafka_error) => {
                if flush_rd_kafka_error == OperationTimedOut {
                    flush(producer, depth + 1);
                }
            }
            _ => {
                eprintln!("Error flushing producer: {:?}", flush_error);
            }
        },
    }
}
