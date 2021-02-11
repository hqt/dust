use crate::network_outbound::NetworkOutbound;
use crate::peer_sender::PeerSender;
use crate::msg::Proposal;
use crate::raft_store::RaftStore;
use crate::poller::Poller;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use raft::{prelude::*};
use protobuf::Message as PbMessage;
use crate::proposal_queue::ProposalQueue;
use crate::network_inbound::NetworkInbound;

// start a raft node
pub fn start<A, B>(
    id: u64,
    network_inbound: NetworkInbound<A, B>,
    network_outbound: NetworkOutbound,
    initialize: bool,
) -> (RaftStore, Poller)
    where
        A: PeerSender<Message=Message> + Send + 'static,
        B: PeerSender<Message=Proposal> + Send + 'static,
{
    let proposal_queue = ProposalQueue::new();
    let raft_store = RaftStore::new(id, proposal_queue.clone());
    let poller = Poller::start(id, proposal_queue, network_inbound, network_outbound, initialize);
    (raft_store, poller)
}

// start a raft node
pub fn start_for_testing<A, B>(
    id: u64,
    network_inbound: NetworkInbound<A, B>,
    network_outbound: NetworkOutbound,
    proposal_queue: ProposalQueue,
    initialize: bool,
) -> (RaftStore, Poller)
    where
        A: PeerSender<Message=Message> + Send + 'static,
        B: PeerSender<Message=Proposal> + Send + 'static,
{
    let raft_store = RaftStore::new(id, proposal_queue.clone());
    let poller = Poller::start(id, proposal_queue, network_inbound, network_outbound, initialize);
    (raft_store, poller)
}
