use std::thread::sleep;
use log::{info};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;
use rand::Rng;
use rdkafka::ClientConfig;
use rdkafka::util::Timeout;

pub(crate) mod generator;
pub(crate) mod powerplant;

#[tokio::main]
async fn main() {
    env_logger::init();

    let broker_urls = vec!["10.99.112.42:9094"];

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", broker_urls.join(","))
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    info!("Created producer");

    let topic_name = "umh.v1.hello-world";

    let mut rng = rand::thread_rng();
    loop {
        // The send operation on the topic returns a future, which will be
        // completed once the result or failure from Kafka is received.
        let msg = format!("Message {}", rng.gen_range(0..1000));
        let record: FutureRecord<String, String> = FutureRecord::to(topic_name)
            .payload(&msg)
            .partition(5)
            .headers(OwnedHeaders::new().insert(Header {
                key: "header_key",
                value: Some("header_value"),
            }));
        let delivery_status = producer
            .send(
                record,
                Timeout::Never,
            )
            .await;

        // This will be executed when the result is received.
        info!("Delivery status for message received: {:?}", delivery_status);
    }

    info!("Awaited futures");

    // Wait for all outstanding messages to be delivered and then shut down.
    producer
        .flush(Duration::from_secs(10))
        .expect("Failed to flush producer");
}
