pub mod chernobyl;

pub struct Message {
    pub topic: String,
    pub value: Vec<u8>,
    pub key: String,
}

pub trait Generator {
    fn new(split_point: usize, number_of_topics: usize) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn get_message(&self) -> anyhow::Result<Message>;
    fn generate_topics(&mut self, number_of_topics: usize) -> anyhow::Result<()>;
}
