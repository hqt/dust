use raft::eraftpb::ConfChange;
use std::sync::mpsc::{SyncSender, Receiver};
use std::sync::mpsc;
use raft::RawNode;
use raft::storage::MemStorage;

struct Person {

}

#[derive(Debug)]
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

pub fn propose(raft_group: &mut RawNode<MemStorage>, proposal: &mut Proposal) {
    println!("debug proposal. leader {} process  {:?}", raft_group.raft.id, proposal);
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some((ref key, ref value)) = proposal.normal {
        let data = format!("put {} {}", key, value).into_bytes();
        println!("proposal_message {:?}", value);
        let _ = raft_group.propose(vec![], data);
    } else if let Some(ref cc) = proposal.conf_change {
        println!("proposal_configuration_change {:?}", cc);
        let _ = raft_group.propose_conf_change(vec![], cc.clone());
    } else if let Some(_transferee) = proposal.transfer_leader {
        // TODO: implement transfer leader.
        unimplemented!();
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    if last_index2 == last_index1 {
        println!("propose fail");
        // Propose failed, don't forget to respond to the client.
        proposal.propose_success.send(false).unwrap();
    } else {
        println!("propose success");
        proposal.proposed = last_index1;
    }
}
