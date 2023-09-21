use crate::generator::Generator;
use anyhow::Result;

trait Sender {
    fn new(brokers: Vec<String>) -> Result<Self>
    where
        Self: Sized;
    fn begin<T: Generator>(
        &mut self,
        split_point: usize,
        topics: usize,
    ) -> Result<()>;
    fn end(&mut self);
    fn get_send_message_hashes(&self) -> Vec<String>;
}

pub mod kafka;
