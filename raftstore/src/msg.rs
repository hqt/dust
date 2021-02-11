use std::sync::mpsc::{SyncSender, Receiver};

use raft::eraftpb::ConfChange;
use std::sync::mpsc;

#[derive(Clone, Debug)]
pub struct Proposal {
    pub normal: Option<(u16, String)>,
    // key is an u16 integer, and value is a string.
    pub conf_change: Option<ConfChange>,
    // conf change.
    pub transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    pub proposed: u64,
    pub propose_success: SyncSender<bool>,
}

impl Proposal {
    pub fn conf_change(cc: &ConfChange) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }

    pub fn normal(key: u16, value: String) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: Some((key, value)),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }
}

