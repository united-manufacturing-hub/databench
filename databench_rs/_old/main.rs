use std::collections::{HashMap, HashSet};
use log::{error, info, warn};



use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use tokio::join;

pub(crate) mod generator;
pub(crate) mod powerplant;

#[tokio::main]
async fn main() {
    env_logger::init();

    test_insert_speed().await;
}

async fn test_insert_speed() {

    let mut broker_urls = vec![];

    // Read brokers from env
    for i in 0..10 {
        // KAFKA_BROKER_URL_<i>
        let env_var = format!("KAFKA_BROKER_URL_{}", i);
        match std::env::var(&env_var) {
            Ok(v) => {
                if v.is_empty(){
                    break;
                }
                info!("Found broker: {}", v);
                broker_urls.push(v);
            }
            Err(_) => {
                break;
            }
        }
    }

    if broker_urls.is_empty() {
        warn!("No brokers specified, using united-manufacturing-hub-kafka-external:9094");
        broker_urls.push("united-manufacturing-hub-kafka-external:9094".to_string());
    }



    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", broker_urls.join(","))
        .set("message.timeout.ms", "50000")
        .create()
        .expect("Producer creation error");

    let n_topic = 10_000;
    let split = 4;
    let n_messages = 1_000_000;

    let mut generator = generator::Generator::new(n_topic, split, n_messages);
    let messages = generator.get_message();

    // Pre-create topics
    info!("Pre-creating topics");
    let topics = generator.get_topics();
    let mut unique_topics = HashSet::new();

    for topic in topics.iter() {
        // Split t and collect first <split> elements
        let topic_name_split: Vec<&str> = topic.name.split(".").collect();

        let topic_name = topic_name_split
            .iter()
            .take(split)
            .cloned()
            .collect::<Vec<_>>()
            .join(".");

        unique_topics.insert(topic_name);
    }

    for topic_name in unique_topics{
        let delivery_status = producer.send(
            FutureRecord::to(&topic_name)
                .payload("")
                .key("")
                .partition(0),
            Timeout::After(std::time::Duration::from_millis(100)),
        );
        match delivery_status.await {
            Ok(v) => {
                info!("Created topic: {} ({:?})", topic_name, v)
            }
            Err(e) => {
                error!("Failed to create topic: {:?}", e)
            }
        }
    }

    info!("Created topics");

    let now = std::time::Instant::now();

    let joinable_futures = messages
        .iter()
        .map(|msg| {
            let record: FutureRecord<String, Vec<u8>> = FutureRecord::to(&msg.topic)
                .payload(&msg.message)
                .key(&msg.key);
            let delivery_status = producer.send(
                record,
                Timeout::After(std::time::Duration::from_secs(60)),
            );
            delivery_status
        })
        .collect::<Vec<_>>();

    info!("Send {} messages in {:?}", n_messages, now.elapsed());

    let results = futures::future::join_all(joinable_futures).await;
    let mut failed = 0;
    let mut success = 0;
    for result in results {
        match result {
            Ok(_) => {
                success += 1;
            }
            Err(e) => {
                failed += 1;
            }
        }
    }

    info!("Awaited all deliveries in {:?}", now.elapsed());
    info!("Success: {}, Failed: {}", success, failed);
}
