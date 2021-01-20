use raft::{prelude::*};
use std::sync::mpsc::Sender;
use std::collections::HashMap;
use std::iter::Map;

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

pub struct Peer<T: PeerSender> {
    senders: HashMap<u64, T>
}

// impl Peer<T> {
//     pub fn new() -> Peer<T> {
//         Peer{ senders: HashMap::new() }
//     }
//
//     pub fn add_sender(mut self, sender: Box<dyn PeerSender>) -> Option<dyn PeerSender> {
//         self.senders.insert(sender.receiver_id(), sender)
//     }
// }

#[cfg(test)]
mod tests {
    #[test]
    fn test_peekable() {
        // let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        // let engines = new_temp_engine(&path);
        // let mut r = Region::default();
        // r.set_id(10);
        // r.set_start_key(b"key0".to_vec());
        // r.set_end_key(b"key4".to_vec());
        // let store = new_peer_storage(engines.clone(), &r);
        //
        // let key3 = b"key3";
        // engines.kv.put_msg(&data_key(key3), &r).expect("");
        //
        // let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);
        // let v3 = snap.get_msg(key3).expect("");
        // assert_eq!(v3, Some(r));
        //
        // let v0 = snap.get_value(b"key0").expect("");
        // assert!(v0.is_none());
        //
        // let v4 = snap.get_value(b"key5");
        // assert!(v4.is_err());
    }
}
