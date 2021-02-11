use crate::node_official;
use std::sync::{mpsc, Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use crate::proposal::Proposal;
use std::thread;
use std::time::Duration;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use raft::{prelude::*};

// acted as the network_tmp layer
pub fn execute() {
    let mut senders = HashMap::new();
    let mut receivers = Vec::new();

    let (sender, receiver, proposals) = create_node(1);
    senders.insert(1, sender);
    receivers.push(receiver);

    let (sender, receiver, _) = create_node(2);
    senders.insert(2, sender);
    receivers.push(receiver);

    // send proposal to leader
    let mut conf_change = ConfChange::default();
    conf_change.node_id = 2;
    conf_change.set_change_type(ConfChangeType::AddNode);
    let (proposal, rx) = Proposal::conf_change(&conf_change);
    proposals.lock().unwrap().push_back(proposal);
    // wait to finish proposal. actually other node does not need to work anything !!!
    if rx.recv().unwrap() {
        println!("done joining");
    }

    thread::spawn(move || {
        // send proposals to write message
        for i in 1..10 { // change it to get range
            println!("send {}", i);
            let (proposal, rx) = Proposal::normal(i, "hello, world".to_owned());
            proposals.lock().unwrap().push_back(proposal);
            // After we got a response from `rx`, we can assume the put succeeded and following
            // `get` operations can find the key-value pair.
            rx.recv().unwrap();
        }

        println!("done sending messages");
    });

    // // send proposals to write message
    // for i in 1..10 { // change it to get range
    //     println!("send {}", i);
    //     let (proposal, rx) = Proposal::normal(i, "hello, world".to_owned());
    //     proposals.lock().unwrap().push_back(proposal);
    //     // After we got a response from `rx`, we can assume the put succeeded and following
    //     // `get` operations can find the key-value pair.
    //     rx.recv().unwrap();
    // }
    //
    // println!("done sending messages");

    loop {
        thread::sleep(Duration::from_millis(100));
        // process receivers
        for (pos, recv) in receivers.iter().enumerate() {
            match recv.try_recv() {
                Ok(msg) => {
                    let id = msg.to;
                    if (msg.get_msg_type() != MessageType::MsgHeartbeatResponse) && (msg.get_msg_type() != MessageType::MsgHeartbeat) {
                        println!("{} send msg to {}: content {:?}", (pos + 1), id, msg);
                    }
                    senders.get(&id).unwrap().send(msg);
                }
                Err(TryRecvError::Empty) => {
                    // break;
                }
                Err(TryRecvError::Disconnected) => {
                    return;
                }
            }
        }
    }
    println!("DONE")
}

pub fn create_node(id: u64) -> (Sender<Message>, Receiver<Message>, Arc<Mutex<VecDeque<Proposal>>>) {
    if id == 1 {
        let proposals = Arc::new(Mutex::new(VecDeque::<Proposal>::new()));
        let (mailboxSender, mailboxReceiver) = mpsc::channel();
        let (outboundSender, outboundReceiver) = mpsc::channel();

        let leader = node_official::Node::new_leader(id, mailboxReceiver, outboundSender, proposals.clone());
        return (mailboxSender, outboundReceiver, proposals.clone());
    } else {
        let proposals = Arc::new(Mutex::new(VecDeque::<Proposal>::new()));
        let (mailboxSender, mailboxReceiver) = mpsc::channel();
        let (outboundSender, outboundReceiver) = mpsc::channel();

        let leader = node_official::Node::new_follower(id, mailboxReceiver, outboundSender);
        return (mailboxSender, outboundReceiver, proposals);
    }
}

