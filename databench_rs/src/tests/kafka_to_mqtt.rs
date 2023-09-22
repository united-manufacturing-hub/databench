
#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, RwLock};
    use std::thread;
    use futures::executor;
    use rdkafka::admin::AdminClient;
    use rdkafka::client::DefaultClientContext;
    use rdkafka::ClientConfig;
    use crate::generator::chernobyl::Chernobyl;
    use crate::sender::kafka::KafkaSender;
    use crate::sender::Sender;
    use crate::receiver::Receiver;
    use crate::receiver::kafka::KafkaReceiver;
    use crate::receiver::mqtt::MQTT3Receiver;
    use crate::tests::Status;

    #[test]
    #[allow(clippy::expect_used)]
    fn test_kafka_sender_kafka_receiver() {
        let kafka_brokers = vec![
            "10.99.112.33:31092".to_string(),
            "10.99.112.34:31092".to_string(),
            "10.99.112.35:31092".to_string(),
        ];
        let mqtt_brokers = vec![
            "10.99.112.33:1883".to_string(),
        ];
        // Remove topic (using admin client)
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers.join(","))
            .create()
            .expect("Failed to create admin client");

        println!("Deleting topic");
        executor::block_on(async {
            admin_client.delete_topics(&["umh.v1.chernobylnuclearpowerplant"], &Default::default()).await.expect("Failed to delete topic");
        });



        #[allow(unused_assignments)]
            let mut send_hashes: Arc<RwLock<VecDeque<String>>> = Arc::new(RwLock::new(VecDeque::new()));
        #[allow(unused_assignments)]
            let mut recv_hashes: Arc<RwLock<VecDeque<String>>> = Arc::new(RwLock::new(VecDeque::new()));

        let recv_hash_recv_thread = recv_hashes.clone();
        let send_hashes_send_thread = send_hashes.clone();
        let recv_thread = thread::spawn(move || mqtt_recv(mqtt_brokers.clone(), recv_hash_recv_thread));
        let sender_thread = thread::spawn(move || kafka_sender(kafka_brokers.clone(), send_hashes_send_thread));

        // wait for both threads to finish
        sender_thread.join().expect("Failed to join sender thread");
        recv_thread.join().expect("Failed to join receiver thread");


        let mut message_map: HashMap<String, Status> = HashMap::new();

        for hash in send_hashes.write().expect("Failed to get read lock").drain(..) {
            message_map.insert(hash, Status::Sent);
        }

        for hash in recv_hashes.write().expect("Failed to get read lock").drain(..) {
            match message_map.entry(hash) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.insert(Status::SentAndReceived);
                },
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(Status::Received);
                },
            }
        }

        let mut not_received = 0;
        let mut received = 0;
        for (hash, status) in message_map.iter() {
            match status {
                Status::Sent => {
                    not_received += 1;
                },
                Status::Received => {
                    // These are from other tests
                },
                Status::SentAndReceived => {
                    received += 1;
                },
            }
        }

        let mut received_percent = 0.0;
        if received > 0 {
            received_percent = received as f64 / (received + not_received) as f64;
        }

        println!("Received {}% of messages [{} of {}]", received_percent * 100.0, received, received + not_received);
    }

    fn mqtt_recv(mqtt_brokers: Vec<String>, recv_hashes: Arc<RwLock<VecDeque<String>>>)
    // Receiver
    {
        println!("Starting MQTT receiver");
        #[allow(clippy::expect_used)]
        let mut receiver = MQTT3Receiver::new(mqtt_brokers, "umh/v1/chernobylnuclearpowerplant/#".to_string())
            .expect("Failed to create receiver");

        #[allow(clippy::expect_used)]
        receiver.begin().expect("Failed to begin receiving");
        for i in 0..120 {
            let recv_cnt = receiver.get_received_messages();
            println!("Waiting for messages [{}]", i);
            thread::sleep(std::time::Duration::from_secs(1));
        }
        receiver.end();

        #[allow(clippy::expect_used)]
        let mut write_lock = recv_hashes.write().expect("Failed to get write lock");
        for v in receiver.get_received_messages_hashes() {
            write_lock.push_back(v);
        }
    }

    fn kafka_sender(kafka_brokers: Vec<String>, send_hashes: Arc<RwLock<VecDeque<String>>>){
        #[allow(clippy::expect_used)]
        let mut sender = KafkaSender::new(kafka_brokers.clone())
            .expect("Failed to create sender");
        let seconds = 5;

        let now = std::time::Instant::now();

        #[allow(clippy::expect_used)]
        sender
            .begin::<Chernobyl>(3, 100)
            .expect("Failed to begin sending");
        thread::sleep(std::time::Duration::from_secs(seconds));
        sender.end();
        let elapsed = now.elapsed();

        // Wait for kafka to catch up
        thread::sleep(std::time::Duration::from_secs(5));

        // Get hashes
        {
            #[allow(clippy::expect_used)]
            let mut send_hashes_lock = send_hashes.write().expect("Failed to get write lock");
            for v in sender.get_sent_message_hashes() {
                send_hashes_lock.push_back(v);
            }
        }


        #[allow(clippy::expect_used)]
        let send_read_lock = send_hashes.read().expect("Failed to get read lock");

        assert!(!send_read_lock.is_empty());

        let msg_per_sec = send_read_lock.len() as f64 / elapsed.as_secs_f64();

        println!(
            "Sent {} ({}/s) messages in {:?}",
            send_read_lock.len(),
            msg_per_sec,
            elapsed
        );
    }
}
