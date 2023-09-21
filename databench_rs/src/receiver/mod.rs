use anyhow::Result;

trait Receiver{
    fn new(brokers: Vec<String>, topic: String) -> Result<Self>
    where
        Self: Sized;
    fn begin(&mut self) -> Result<()>;
    fn end(&mut self);
    fn get_send_message_hashes(&self) -> Vec<String>;
}

pub mod kafka;
