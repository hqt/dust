use raft::{RawNode, Config};
use crate::network_outbound::NetworkOutbound;
use crate::peer_sender::PeerSender;
use crate::msg::Proposal;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use protobuf::Message as PbMessage;
use raft::{prelude::*, StateRole};
use crate::proposal_queue::ProposalQueue;

pub trait RaftEngine {
    fn insert_data(&mut self);
    fn become_leader(&mut self);
}

pub struct RaftStore {
    id: u64,
    proposal_queue: ProposalQueue,
}

impl RaftStore {
    pub fn new(id: u64, proposal_queue: ProposalQueue) -> Self {
        RaftStore {
            id,
            proposal_queue,
        }
    }
    fn insert_data(&mut self) {
        unimplemented!()
    }

    pub fn join(&mut self, leader_id: u64) {

    }
}

