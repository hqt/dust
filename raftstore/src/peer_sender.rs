use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender, SendError};
use std::collections::HashMap;

use raft::{prelude::*};

// interface represent the ability send data to Raft's node
pub trait PeerSender {
    type Message;
    fn send(&self, msg: Self::Message) -> Result<(), SendError<Self::Message>>;
    fn sender_id(&self) -> u64;
    fn receiver_id(&self) -> u64;
}

// ChannelSender using Rust:mpsc mechanism for testing
#[derive(Clone, Debug)]
pub struct ChannelSender<T> {
    sender_id: u64,
    receiver_id: u64,
    sender: Sender<T>,
}

impl<T> ChannelSender<T> {
    pub fn new(sender_id: u64, receiver_id: u64) -> (ChannelSender<T>, Receiver<T>) {
        let (tx, rx) = mpsc::channel::<T>();
        let sender = ChannelSender {
            sender_id,
            receiver_id,
            sender: tx,
        };
        (sender, rx)
    }
}

impl<T> PeerSender for ChannelSender<T> {
    type Message = T;

    fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.sender.send(msg)
    }

    fn sender_id(&self) -> u64 {
        self.sender_id
    }

    fn receiver_id(&self) -> u64 {
        self.receiver_id
    }
}

impl<T> Drop for ChannelSender<T> {
    fn drop(&mut self) {
        println!("channel sender: drop from {} to {}", self.sender_id, self.receiver_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_sender() {
        let (sender, receiver) = ChannelSender::<Message>::new(1, 2);
        let mut msg = Message::new();
        msg.index = 10;
        sender.send(msg.clone());
        let result = receiver.recv().unwrap();
        assert_eq!(result.index, 10);
        assert_eq!(sender.receiver_id(), 2);
        assert_eq!(sender.sender_id(), 1);
    }
}
