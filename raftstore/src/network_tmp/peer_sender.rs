use raft::{prelude::*};
use std::sync::mpsc::Sender;
use std::collections::HashMap;

// interface represent the ability send data to Raft's node
pub trait PeerSender {
    fn send(self, msg: Message);
    fn sender_id(self) -> u64;
    fn receiver_id(self) -> u64;
}

// ChannelSender using Rust:mpsc mechanism, for testing
pub struct ChannelSender {
    sender_id: u64,
    receiver_id: u64,
    sender: Sender<Message>,
}

impl ChannelSender {
    pub fn new(sender_id: u64, receiver_id: u64, sender: Sender<Message>) -> Self {
        ChannelSender {
            sender_id,
            receiver_id,
            sender,
        }
    }
}

// impl PeerSender for ChannelSender {
//     fn send(self, msg: Message) {
//         if (msg.get_msg_type() != MessageType::MsgHeartbeatResponse) && (msg.get_msg_type() != MessageType::MsgHeartbeat) {
//             let to = msg.to;
//             println!("{} send msg to {}: content {:?}", self.sender_id(), to, msg);
//         }
//         self.sender.send(msg);
//     }
//
//     fn sender_id(self) -> u64 {
//         self.sender_id
//     }
//
//     fn receiver_id(self) -> u64 {
//         self.receiver_id
//     }
// }

