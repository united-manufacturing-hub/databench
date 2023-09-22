use crate::receiver::Receiver;
use rumqttc::{Event, Incoming, MqttOptions};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::sleep;

pub(crate) struct MQTT3Receiver {
    broker: String,
    port: u16,
    topic: String,
    receiving: Arc<AtomicBool>,
    hashes: Arc<RwLock<VecDeque<String>>>,
    received_message_cnt: Arc<AtomicU64>,
}

impl Receiver for MQTT3Receiver {
    fn new(brokers: Vec<String>, topic: String) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        if brokers.len() != 1 {
            return Err(anyhow::anyhow!("Only one broker is supported for MQTT"));
        }
        let split_pos = match brokers[0].find(':') {
            None => {
                return Err(anyhow::anyhow!(
                    "Broker address must be in the format <address>:<port>"
                ))
            }
            Some(v) => {v}
        };

        let (broker, port) = brokers[0].split_at(split_pos);
        let port = &port[1..];

        Ok(Self {
            broker: broker.to_string(),
            port: port.parse::<u16>()?,
            topic,
            receiving: Arc::new(AtomicBool::new(false)),
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

        let mqttoptions = MqttOptions::new("databench", self.broker.as_str(), self.port);

        let (mut mqtt_client, mut notifications) = rumqttc::Client::new(mqttoptions, 1000);
        mqtt_client.subscribe(self.topic.as_str(), rumqttc::QoS::AtLeastOnce)?;

        let hashes = self.hashes.clone();
        let received_messages_counter = self.received_message_cnt.clone();

        thread::spawn(move || {
            let mut hasher = blake3::Hasher::new();
            let mut thread_hashes = VecDeque::new();

            while receiving.load(Ordering::Relaxed) {
                match notifications.recv_timeout(std::time::Duration::from_millis(1000)) {
                    Err(_) => {
                        sleep(std::time::Duration::from_millis(1000));
                        continue;
                    }
                    Ok(event_result) => {
                        match event_result {
                            Ok(event) => {
                                match event {
                                    Event::Incoming(incoming_event) => {
                                        if let Incoming::Publish(publish) = incoming_event {
                                            hasher.reset();
                                            let topic = publish.topic.replace('/', ".");
                                            hasher.update(topic.as_bytes());
                                            hasher.update(&publish.payload);

                                            let raw_hash = hasher.finalize();
                                            let hash = hex::encode(raw_hash.as_bytes());

                                            thread_hashes.push_back(hash);
                                            received_messages_counter
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    Event::Outgoing(_) => {
                                        // Ignore outgoing events
                                    }
                                }
                            }
                            Err(conn_error) => {
                                eprintln!("Connection error: {:?}", conn_error);
                                continue;
                            }
                        }
                    }
                }

                if received_messages_counter.load(Ordering::Relaxed) % 10000 == 0 {
                    println!(
                        "Received {} messages",
                        received_messages_counter.load(Ordering::Relaxed)
                    );
                    let hashlock = hashes.write();
                    #[allow(clippy::expect_used)]
                    let mut hashes = hashlock.expect("Failed to get write lock");
                    for hash in thread_hashes.drain(..) {
                        hashes.push_back(hash);
                    }
                    thread_hashes.clear();
                }
            }

            println!(
                "Received {} messages",
                received_messages_counter.load(Ordering::Relaxed)
            );
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
