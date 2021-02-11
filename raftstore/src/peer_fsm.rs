use protobuf::Message as PbMessage;
use raft::{prelude::*, StateRole};
use crate::network_outbound::NetworkOutbound;
use crate::msg::Proposal;
use crate::peer_sender::PeerSender;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use crate::proposal_queue::ProposalQueue;
use crate::network_inbound::NetworkInbound;
use raft::storage::MemStorage;

pub trait Fsm {}

pub struct PeerFsm<A: PeerSender<Message=Message>, B: PeerSender<Message=Proposal>>
{
    pub id: u64,
    pub raft_group: RawNode<MemStorage>,
    network_inbound: NetworkInbound<A, B>,
    proposal_queue: ProposalQueue,
}

impl<A, B> PeerFsm<A, B>
    where
        A: PeerSender<Message=Message>,
        B: PeerSender<Message=Proposal>,
{
    pub fn new(id: u64, raft_group: RawNode<MemStorage>, network_inbound: NetworkInbound<A, B>, proposal_queue: ProposalQueue) -> Self {
        PeerFsm { id, network_inbound, proposal_queue, raft_group }
    }

    pub fn tick(&mut self) {
        self.raft_group.tick();
    }

    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    pub fn on_peer_message(&mut self, msg: Message) {
        self.raft_group.step(msg);
    }

    pub fn on_ready(&mut self) {
        let mut raft_group = &mut self.raft_group;
        if !raft_group.has_ready() {
            return;
        }

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = raft_group.ready();

        let store = raft_group.raft.raft_log.store.clone();

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize raft logs to the latest position.
        if let Err(e) = store.wl().append(ready.entries()) {
            eprintln!("persist raft log fail: {:?}, need to retry or panic", e);
            return;
        }

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = store.wl().apply_snapshot(s) {
                eprintln!("apply snapshot fail: {:?}, need to retry or panic", e);
                return;
            }
        }

        // Send out the internal messages come from the node.
        for msg in ready.messages.drain(..) {
            let to = msg.to;
            let sender = self.network_inbound.get_internal_sender(to);
            if sender.is_some() {
                // println!("peer_fsm: send from {} to {}. msg: {:?}", self.peer.id, to, msg);
                let res = sender.unwrap().send(msg);
            } else {
                println!("FUUUUUUUUUUUUUUUUCK");
            }
            // if self.conn_manager.send_to(to, msg).is_err() {
            //     eprintln!("send raft message to {} fail, let Raft retry it", to);
            // }
        }

        // Apply all committed proposals.
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                println!("entry: {:?}", entry);
                // When the peer becomes Leader it will send an empty entry.
                if entry.data.is_empty() {
                    continue;
                }

                if let EntryType::EntryConfChange = entry.get_entry_type() {
                    println!("node: {} content:{:?}", self.id, entry);
                    // For conf change messages, make them effective.
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let node_id = cc.node_id;
                    match cc.get_change_type() {
                        ConfChangeType::AddNode => raft_group.raft.add_node(node_id).unwrap(),
                        ConfChangeType::RemoveNode => raft_group.raft.remove_node(node_id).unwrap(),
                        ConfChangeType::AddLearnerNode => raft_group.raft.add_learner(node_id).unwrap(),
                        ConfChangeType::BeginMembershipChange
                        | ConfChangeType::FinalizeMembershipChange => unimplemented!(),
                    }
                    let cs = ConfState::from(raft_group.raft.prs().configuration().clone());
                    store.wl().set_conf_state(cs, None);
                } else {
                    // For normal proposals, extract the key-value pair and then
                    // insert them into the kv engine.

                    // TODO implement to add to DB
                    // let data = str::from_utf8(&entry.data).unwrap();
                    // let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
                    // if let Some(caps) = reg.captures(&data) {
                    //     // TODO
                    //     self.fsm_callback.on_committed_entry(data.as_ref());
                    //     // kv_pairs.insert(caps[1].parse().unwrap(), caps[2].to_string());
                    // }
                }

                // TODO work here
                if raft_group.raft.state == StateRole::Leader {
                    // The leader should response to the clients, tell them if their proposals succeeded or not.
                    println!("proposal size: {}", self.proposal_queue.get_ref().lock().unwrap().len());
                    if let Some(proposal) = self.proposal_queue.remove_proposal() {
                        println!("leader: pullout proposal. {:?}", proposal);
                        proposal.propose_success.send(true).unwrap();
                    }
                }
            }

            if let Some(last_committed) = committed_entries.last() {
                let mut s = store.wl();
                s.mut_hard_state().commit = last_committed.index;
                s.mut_hard_state().term = last_committed.term;
            }
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        raft_group.advance(ready);
    }

    pub fn on_proposal_normal(&mut self, proposal: &mut Proposal) {
        if let Some((ref key, ref value)) = proposal.normal {
            let last_index1 = self.raft_group.raft.raft_log.last_index() + 1;
            let data = format!("put {} {}", key, value).into_bytes();
            println!("proposal_message {:?}", value);
            let result = self.raft_group.propose(vec![], data);

            let last_index2 = self.raft_group.raft.raft_log.last_index() + 1;
            if last_index2 == last_index1 {
                println!("propose fail");
                // Propose failed, don't forget to respond to the client.
                proposal.propose_success.send(false).unwrap();
            } else {
                println!("normal propose: propose success -> {}", last_index1);
                proposal.proposed = last_index1;
            }
        }
    }

    pub fn on_proposal_cfg_change(&mut self, proposal: &mut Proposal) {
        if let Some(ref cc) = proposal.conf_change {
            let last_index1 = self.raft_group.raft.raft_log.last_index() + 1;

            println!("proposal_configuration_change {:?}", cc);
            let res = self.raft_group.propose_conf_change(vec![], cc.clone());

            let last_index2 = self.raft_group.raft.raft_log.last_index() + 1;
            if last_index2 == last_index1 {
                println!("propose fail");
                // Propose failed, don't forget to respond to the client.
                proposal.propose_success.send(false).unwrap();
            } else {
                println!("cfg change propose: propose success -> {}", last_index1);
                proposal.proposed = last_index1;
            }
        }
    }
}