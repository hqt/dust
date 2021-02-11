use std::collections::HashMap;
use crate::peer_sender::PeerSender;
use std::sync::{Arc, Mutex};

use raft::{prelude::*};
use std::borrow::Borrow;

// ConnManager manages network inbound: either to other Raft nodes or "clients" (configuration change)
#[derive(Clone)]
pub struct NetworkInbound<InternalSender, ProposalSender: PeerSender> {
    // mapping from receiver_id to Sender
    internal_senders: HashMap<u64, InternalSender>,
    proposal_senders: HashMap<u64, ProposalSender>,
    lock: Arc<Mutex<u64>>,
}

impl<InternalSender: PeerSender, ProposalSender: PeerSender> NetworkInbound<InternalSender, ProposalSender> {
    pub fn new() -> Self {
        NetworkInbound {
            internal_senders: Default::default(),
            proposal_senders: Default::default(),
            lock: Arc::new(Mutex::new(0)),
        }
    }

    fn drop(&mut self) {
        println!("network_inbound. fucking drop");
    }

    pub fn add_conn(&mut self, internal_sender: InternalSender, proposal_sender: ProposalSender) {
        assert_eq!(internal_sender.sender_id(), proposal_sender.sender_id());
        assert_eq!(internal_sender.receiver_id(), proposal_sender.receiver_id());
        let receiver_id = internal_sender.receiver_id();

        self.lock.lock();
        self.internal_senders.insert(receiver_id, internal_sender);
        self.proposal_senders.insert(receiver_id, proposal_sender);
    }

    pub fn remove_conn(&mut self, receiver_id: u64) {
        self.lock.lock();
        self.internal_senders.remove(&receiver_id);
        self.proposal_senders.remove(&receiver_id);
    }

    pub fn get_internal_sender(&self, receiver_id: u64) -> Option<&InternalSender> {
        self.lock.lock();
        self.internal_senders.get(&receiver_id)
    }

    pub fn get_proposal_sender(&self, receiver_id: u64) -> Option<&ProposalSender> {
        self.lock.lock();
        self.proposal_senders.get(&receiver_id)
    }

    pub fn size(&self) -> usize {
        self.internal_senders.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_sender::ChannelSender;

    #[test]
    fn test_conn_manager() {
        let (sender1, receiver1) = ChannelSender::<Message>::new(1, 1);
        let (sender2, receiver2) = ChannelSender::<Message>::new(2, 2);

        let mut conn_manager = NetworkInbound::<ChannelSender<Message>, ChannelSender<Message>>::new();

        // insert connection
        conn_manager.add_conn(sender1.clone(), sender1.clone());
        conn_manager.add_conn(sender2.clone(), sender2.clone());
        conn_manager.add_conn(sender2.clone(), sender2.clone());
        assert_eq!(conn_manager.size(), 2);

        // get connection
        let proposal_sender = conn_manager.get_proposal_sender(1);
        assert_eq!(proposal_sender.is_some(), true);
        assert_eq!(proposal_sender.unwrap().sender_id(), 1);

        let internal_sender = conn_manager.get_internal_sender(1);
        assert_eq!(internal_sender.is_some(), true);
        assert_eq!(internal_sender.unwrap().sender_id(), 1);

        // remove none existed key
        conn_manager.remove_conn(999);
        assert_eq!(conn_manager.size(), 2);

        // remove connection
        conn_manager.remove_conn(1);
        assert_eq!(conn_manager.size(), 1);

        let proposal_sender = conn_manager.get_proposal_sender(1);
        assert_eq!(proposal_sender.is_none(), true);
        let internal_sender = conn_manager.get_internal_sender(1);
        assert_eq!(internal_sender.is_none(), true);
    }

    #[test]
    #[should_panic]
    fn test_conn_manager_exception() {
        let (sender1, receiver1) = ChannelSender::<Message>::new(1, 1);
        let (sender2, receiver2) = ChannelSender::<Message>::new(2, 2);

        let mut conn_manager = NetworkInbound::<ChannelSender<Message>, ChannelSender<Message>>::new();
        conn_manager.add_conn(sender1.clone(), sender2.clone());
    }
}