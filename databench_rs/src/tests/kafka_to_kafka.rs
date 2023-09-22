
#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
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
    use crate::tests::Status;

    #[test]
    #[allow(clippy::expect_used)]
    fn test_kafka_sender_kafka_receiver() {
        let brokers = vec![
            "10.99.112.33:31092".to_string(),
            "10.99.112.34:31092".to_string(),
            "10.99.112.35:31092".to_string(),
        ];
        // Remove topic (using admin client)
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            .create()
            .expect("Failed to create admin client");

        println!("Deleting topic");
        executor::block_on(async {
            admin_client.delete_topics(&["umh.v1.chernobylnuclearpowerplant"], &Default::default()).await.expect("Failed to delete topic");
        });



        #[allow(unused_assignments)]
            let mut send_hashes: VecDeque<String> = VecDeque::new();
        #[allow(unused_assignments)]
            let mut recv_hashes: VecDeque<String> = VecDeque::new();
        #[allow(unused_assignments)]
            let mut sent_cnt = 0;
        // Sender
        {
            let mut sender = KafkaSender::new(brokers.clone())
                .expect("Failed to create sender");
            let seconds = 5;

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
            send_hashes = sender.get_sent_message_hashes();
            assert!(!send_hashes.is_empty());

            let msg_per_sec = send_hashes.len() as f64 / elapsed.as_secs_f64();

            println!(
                "Sent {} ({}/s) messages in {:?}",
                send_hashes.len(),
                msg_per_sec,
                elapsed
            );
            sent_cnt = sender.get_sent_messages();
        }

        // Receiver
        {
            let mut receiver = KafkaReceiver::new(brokers, "umh.v1.chernobylnuclearpowerplant".to_string())
                .expect("Failed to create receiver");

            receiver.begin().expect("Failed to begin receiving");
            for i in 0..120 {
                let recv_cnt = receiver.get_received_messages();
                if recv_cnt == sent_cnt {
                    println!("Received all messages");
                    break;
                }
                println!("Waiting for messages [{}]", i);
                thread::sleep(std::time::Duration::from_secs(1));
            }
            receiver.end();

            recv_hashes = receiver.get_received_messages_hashes();
        }

        let mut message_map: HashMap<String, Status> = HashMap::new();

        for hash in send_hashes.drain(..) {
            message_map.insert(hash, Status::Sent);
        }

        for hash in recv_hashes.drain(..) {
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


}
