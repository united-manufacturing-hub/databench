pub mod kafka_to_kafka;
pub mod kafka_to_mqtt;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Status {
    Sent,
    Received,
    SentAndReceived,
}
