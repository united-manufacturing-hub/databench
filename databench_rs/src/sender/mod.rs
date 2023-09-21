use anyhow::Result;
use crate::generator::Generator;

trait Sender{
    fn new(brokers: Vec<String>) -> Result<Self> where Self: Sized;
    fn begin<T: Generator>(&mut self, messages_per_second: u32, split_point: usize, topics: usize) -> Result<()>;
    fn end(&mut self);
    fn get_send_message_hashes(&self) -> Vec<u64>;
}

pub mod kafka;
