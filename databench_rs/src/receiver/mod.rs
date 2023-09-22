use std::collections::VecDeque;
use anyhow::Result;

pub(crate) trait Receiver {
    fn new(brokers: Vec<String>, topic: String) -> Result<Self>
    where
        Self: Sized;
    fn begin(&mut self) -> Result<()>;
    fn end(&mut self);
    fn get_received_messages_hashes(&self) -> VecDeque<String>;
    fn get_received_messages(&self) -> u64;
}

pub mod kafka;
