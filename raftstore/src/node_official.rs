use std::thread::JoinHandle;
use std::sync::{Arc, Mutex, mpsc};
use std::collections::{VecDeque, HashMap};
use raft::RawNode;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use protobuf::Message as PbMessage;
use raft::eraftpb::ConfState;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use regex::Regex;
use crate::proposal::Proposal;
use crate::{proposal, batch};
use std::{str, thread};
use std::time::{Instant, Duration};
use crate::batch::Mailbox;

pub(crate) struct Node {
    core: Arc<Mutex<CoreNode>>,
    id: u64,
}

struct CoreNode {
    id: u64,
    // None if the raft is not initialized.
    raft_group: Option<RawNode<MemStorage>>,
    // internal messages from other nodes
    my_mailbox: Receiver<Message>,
    // internal messages for sending outbound
    outbound_sender: Sender<Message>,
    // received proposals from other nodes (joining) or clients
    proposals: Arc<Mutex<VecDeque<proposal::Proposal>>>,
}

fn default_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    }
}

impl Node {
    pub fn new_leader(
        id: u64,
        my_mailbox: Receiver<Message>,
        outbound_sender: Sender<Message>,
        proposals: Arc<Mutex<VecDeque<proposal::Proposal>>>,
    ) -> Self {

        // temporary check
        let mut cfg = default_config();
        cfg.id = id;

        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().nodes = vec![1];

        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();

        let raft_group = Some(RawNode::new(&cfg, storage).unwrap());

        let core = Arc::new(Mutex::new(CoreNode {
            id,
            raft_group,
            my_mailbox,
            outbound_sender,
            proposals,
        }));

        let c1 = core.clone();
        thread::spawn(move || {
            c1.lock().unwrap().run();
            return;
        });

        Node {
            core,
            id,
        }
    }

    pub fn new_follower(
        id: u64,
        my_mailbox: Receiver<Message>,
        outbound_sender: Sender<Message>,
    ) -> Self {
        let core = Arc::new(Mutex::new(CoreNode {
            id,
            raft_group: None,
            my_mailbox,
            outbound_sender,
            proposals: Arc::new(Mutex::new(VecDeque::default())),
        }));

        let c1 = core.clone();
        thread::spawn(move || {
            c1.lock().unwrap().run();
            return;
        });

        Node {
            core,
            id,
        }
    }

    // pub fn is_leader(&self) -> bool {
    //     return self.raft_group.lock().unwrap().raft.state == StateRole::Leader;
    //     // return self.core.lock().unwrap().raft_group.unwrap().raft.state == StateRole::Leader;
    // }
}

impl CoreNode {
    pub fn run(&mut self) {
        thread::sleep(Duration::from_millis(100));
        let mut t = Instant::now();

        // processing step messages
        loop {
            match self.my_mailbox.try_recv() {
                Ok(msg) => {
                    self.step(msg);
                }
                Err(TryRecvError::Empty) => {
                    // println!("{} fucking break", self.id);
                    // break
                }
                Err(TryRecvError::Disconnected) => {
                    println!("fucking return");
                    return;
                }
            }

            let raft_group = match self.raft_group {
                Some(ref mut r) => r,
                // When Node::raft_group is `None` it means the node is not initialized.
                // TODO: consider later
                _ => continue,
            };

            // let is_leader = raft_group.raft.state == StateRole::Leader;
            // if !is_leader {
            //     println!("id: {} is leader: {}", raft_group.raft.id, is_leader);
            // }

            if t.elapsed() >= Duration::from_millis(100) {
                // Tick the raft.
                raft_group.tick();
                t = Instant::now();
            }

            // handle messages from the proposal queue
            // TODO: must be leader ?
            if raft_group.raft.state == StateRole::Leader {
                let mut proposals = self.proposals.lock().unwrap();
                for p in proposals.iter_mut().skip_while(|p| p.proposed > 0) {
                    proposal::propose(raft_group, p);
                }
            }

            // Handle readies from the raft.
            self.on_ready();
        }
    }

    fn step(&mut self, msg: Message) {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg);
            } else {
                return;
            }
        }
        if msg.msg_type != MessageType::MsgHeartbeatResponse && msg.msg_type != MessageType::MsgHeartbeat {
            println!("node {} received {:?}", self.id, msg);
        }

        let raft_group = self.raft_group.as_mut().unwrap();
        let _ = raft_group.step(msg);
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = default_config();
        cfg.id = msg.to;
        let storage = MemStorage::new();
        // self.raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());
        self.raft_group = Some(RawNode::new(&cfg, storage).unwrap());
    }

    fn on_ready(&mut self) {
        let mut raft_group = self.raft_group.as_mut().unwrap();
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

        // Send out the messages come from the node.
        for msg in ready.messages.drain(..) {
            let to = msg.to;
            if self.outbound_sender.send(msg).is_err() {
                eprintln!("send raft message to {} fail, let Raft retry it", to);
            }
        }

        // Apply all committed proposals.
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }
                if let EntryType::EntryConfChange = entry.get_entry_type() {
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
                    let data = str::from_utf8(&entry.data).unwrap();
                    let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
                    if let Some(caps) = reg.captures(&data) {
                        // TODO
                        // kv_pairs.insert(caps[1].parse().unwrap(), caps[2].to_string());
                    }
                }

                if raft_group.raft.state == StateRole::Leader {
                    // The leader should response to the clients, tell them if their proposals succeeded or not.
                    let proposal = self.proposals.lock().unwrap().pop_front().unwrap();
                    println!("leader: pullout proposal. {:?}", proposal);
                    proposal.propose_success.send(true).unwrap();
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
}

// The message can be used to initialize a raft node or not.
fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)
}

enum Signal {
    Terminate,
}

fn check_signals(receiver: &Arc<Mutex<mpsc::Receiver<Signal>>>) -> bool {
    loop {
        match receiver.lock().unwrap().try_recv() {
            Ok(Signal::Terminate) => return true,
            Err(TryRecvError::Empty) => return false,
            Err(TryRecvError::Disconnected) => return true,
        }
    }
}

