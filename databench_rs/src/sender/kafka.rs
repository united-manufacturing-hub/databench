use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use anyhow::Result;
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use crate::generator::Generator;
use crate::sender::Sender;

struct KafkaSender{
    sending: Arc<AtomicBool>,
    brokers: Vec<String>,
}

impl Sender for KafkaSender{
    fn new(brokers: Vec<String>) -> Result<Self>{
        return Ok(Self{
            brokers,
            sending: Arc::new(AtomicBool::new(false)),
        });
    }

    fn begin<T: Generator>(&mut self, messages_per_second: u32, split_point: usize, topics: usize) -> Result<()>{
        // If already sending, return
        if self.sending.swap(true, Ordering::Relaxed){
            return Ok(());
        }

        let min_time_per_message = std::time::Duration::from_secs(1) / messages_per_second;

        // Create a new thread, which sends up to messages_per_second messages per second
        let sending = self.sending.clone();

        // Init kafka producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.join(","))
            .set("message.timeout.ms", "50000")
            .create()?;

        thread::spawn(move || {
            #[allow(clippy::expect_used)]
            let mut generator = T::new(split_point, topics).expect("Failed to create generator");
            let mut last = std::time::Instant::now();
            while sending.load(Ordering::Relaxed){
                // Sleep until the next message should be sent
                let now = std::time::Instant::now();
                let elapsed = now - last;
                if elapsed < min_time_per_message{
                    thread::sleep(min_time_per_message - elapsed);
                }

                let message = generator.get_message();
                match message {
                    Ok(msg) => {
                        // Send the message
                        match producer.send(
                            BaseRecord::to(&msg.topic)
                                .payload(&msg.value)
                                .key(&msg.key),
                        ) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Error sending message: {:?}", e);
                                continue;
                            }
                        };
                    }
                    Err(err) => {
                        eprintln!("Error generating message: {:?}", err);
                        continue;
                    }
                }

                producer.poll(Timeout::After(std::time::Duration::from_millis(100)));

                last = std::time::Instant::now();
            }
        });

        Ok(())
    }

    fn end(&mut self){
        self.sending.store(false, Ordering::Relaxed);
    }

    fn get_send_message_hashes(&self) -> Vec<u64>{
        todo!()
    }
}


#[cfg(test)]
mod tests{
    use crate::generator::chernobyl::Chernobyl;
    use super::*;

    #[test]
    #[allow(clippy::expect_used)]
    fn test_kafka_sender(){
        let mut sender = KafkaSender::new(vec![
            "10.99.112.33:31092".to_string(),
            "10.99.112.34:31092".to_string(),
            "10.99.112.35:31092".to_string(),
        ]).expect("Failed to create sender");
        sender.begin::<Chernobyl>(100, 100, 100).expect("Failed to begin sending");
        thread::sleep(std::time::Duration::from_secs(1));
        sender.end();
    }
}
