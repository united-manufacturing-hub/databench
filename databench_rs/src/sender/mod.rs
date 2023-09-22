use crate::generator::Generator;
use anyhow::Result;
use std::collections::VecDeque;

pub(crate) trait Sender {
    fn new(brokers: Vec<String>) -> Result<Self>
    where
        Self: Sized;
    fn begin<T: Generator>(&mut self, split_point: usize, topics: usize) -> Result<()>;
    fn end(&mut self);
    fn get_sent_message_hashes(&self) -> VecDeque<String>;
    fn get_sent_messages(&self) -> u64;
}

pub mod kafka;
