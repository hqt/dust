use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::collections::{HashMap, VecDeque};
use std::time::{Instant, Duration};
use std::{str, thread};
use std::sync::{Arc, Mutex, mpsc};
use crate::proposal;
use std::thread::JoinHandle;

use protobuf::Message as PbMessage;
use raft::eraftpb::ConfState;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use regex::Regex;
use crate::proposal::Proposal;

struct Node {
    // core: Arc<Mutex<CoreNode>>,
    thread: JoinHandle<()>,
}

struct CoreNode {
    // None if the raft is not initialized.
    raft_group: Option<RawNode<MemStorage>>,
    my_mailbox: Receiver<Message>,
    mailboxes: HashMap<u64, Sender<Message>>,
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
    fn new(
        id: u64,
        my_mailbox: Receiver<Message>,
        mailboxes: HashMap<u64, Sender<Message>>,
        proposals: Arc<Mutex<VecDeque<Proposal>>>,
    ) -> Self {
        let mut raft_group: Option<RawNode<MemStorage>> = None;

        // leader
        if id > 0 {
            let mut cfg = default_config();
            cfg.id = id;
            let mut s = Snapshot::default();
            // Because we don't use the same configuration to initialize every node, so we use
            // a non-zero index to force new followers catch up logs by snapshot first, which will
            // bring all nodes to the same initial state.
            s.mut_metadata().index = 1;
            s.mut_metadata().term = 1;
            s.mut_metadata().mut_conf_state().nodes = vec![1];
            let storage = MemStorage::new();
            storage.wl().apply_snapshot(s).unwrap();
            raft_group = Some(RawNode::new(&cfg, storage).unwrap());
            // raft_group = Some(RawNode::new(&cfg, storage).unwrap());
        }

        // let proposals = Arc::new(Mutex::new(VecDeque::<proposal::Proposal>::new()));

        let core = CoreNode {
            raft_group,
            my_mailbox,
            mailboxes,
            proposals,
        };

        let thread = thread::spawn(move || {
            core.run();
            return;
        });

        Node {
            thread,
        }
    }
}

impl CoreNode {
    pub fn run(mut self) {
        let mut t = Instant::now();
        loop {
            thread::sleep(Duration::from_millis(10));

            // processing step messages
            loop {
                match self.my_mailbox.try_recv() {
                    Ok(msg) => self.step(msg),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            let raft_group = match self.raft_group {
                Some(ref mut r) => r,
                // When Node::raft_group is `None` it means the node is not initialized.
                _ => continue,
            };

            if t.elapsed() >= Duration::from_millis(100) {
                // Tick the raft.
                raft_group.tick();
                t = Instant::now();
            }

            // handle messages from the proposal queue
            if raft_group.raft.state == StateRole::Leader {
                let mut proposals = self.proposals.lock().unwrap();
                for p in proposals.iter_mut().skip_while(|p| p.proposed > 0) {
                    println!("process proposal ...");
                    proposal::propose(raft_group, p);
                }
            }

            // Handle readies from the raft.
            on_ready(
                raft_group,
                // &mut self.kv_pairs,
                &self.mailboxes,
                &self.proposals,
            );

            // Check control signals from
            // if check_signals(&rx_stop_clone) {
            //     return;
            // };
        }
    }

    // Step: use for processing internal messages between nodes
    // initialize the raft if need.
    fn step(&mut self, msg: Message) {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg);
            } else {
                return;
            }
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

fn on_ready(
    raft_group: &mut RawNode<MemStorage>,
    mailboxes: &HashMap<u64, Sender<Message>>,
    proposals: &Mutex<VecDeque<Proposal>>,
) {
    if !raft_group.has_ready() {
        return;
    }
    let store = raft_group.raft.raft_log.store.clone();

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();

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
        if mailboxes[&to].send(msg).is_err() {
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
                println!("on ready: i am leader");
                // The leader should response to the clients, tell them if their proposals succeeded or not.
                let proposal = proposals.lock().unwrap().pop_front().unwrap();
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

pub(crate) fn execute() {
    const NUM_NODES: u32 = 5;
    // Create 5 mailboxes to send/receive messages. Every node holds a `Receiver` to receive
    // messages from others, and uses the respective `Sender` to send messages to others.
    let (mut tx_vec, mut rx_vec) = (Vec::new(), Vec::new());
    for _ in 0..NUM_NODES {
        let (tx, rx) = mpsc::channel();
        tx_vec.push(tx);
        rx_vec.push(rx);
    }

    // let (tx_stop, rx_stop) = mpsc::channel();
    // let rx_stop = Arc::new(Mutex::new(rx_stop));

    // A global pending proposals queue. New proposals will be pushed back into the queue, and
    // after it's committed by the raft cluster, it will be poped from the queue.
    let proposals = Arc::new(Mutex::new(VecDeque::<Proposal>::new()));

    // let mut handles = Vec::new();
    for (i, rx) in rx_vec.into_iter().enumerate() {
        // A map[peer_id -> sender]. In the example we create 5 nodes, with ids in [1, 5].
        let mailboxes = (1..6u64).zip(tx_vec.iter().cloned()).collect();
        let mut node = match i {
            // Peer 1 is the leader.
            0 => Node::new(1, rx, mailboxes, proposals.clone()),
            // Other peers are followers.
            _ => Node::new(0, rx, mailboxes, proposals.clone()),
        };
    }

    // Propose some conf changes so that followers can be initialized.
    add_all_followers(proposals.as_ref());

    // Put 100 key-value pairs.
    println!("We get a 5 nodes Raft cluster now, now propose 100 proposals");
    (0..100u16)
        .filter(|i| {
            let (proposal, rx) = Proposal::normal(*i, "hello, world".to_owned());
            proposals.lock().unwrap().push_back(proposal);
            // After we got a response from `rx`, we can assume the put succeeded and following
            // `get` operations can find the key-value pair.
            rx.recv().unwrap()
        })
        .count();

    println!("Propose 100 proposals success!");
}

// Proposes some conf change for peers [2, 5].
fn add_all_followers(proposals: &Mutex<VecDeque<Proposal>>) {
    for i in 2..6u64 {
        let mut conf_change = ConfChange::default();
        conf_change.node_id = i;
        conf_change.set_change_type(ConfChangeType::AddNode);
        loop {
            let (proposal, rx) = Proposal::conf_change(&conf_change);
            proposals.lock().unwrap().push_back(proposal);
            if rx.recv().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}