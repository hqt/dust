use crate::peer_sender::ChannelSender;

use raft::{prelude::*};
use crate::network_inbound::NetworkInbound;
use crate::msg::Proposal;
use std::sync::mpsc::{Receiver, Sender, RecvTimeoutError, RecvError, SyncSender, TryRecvError};
use std::collections::HashMap;
use crate::network_outbound::NetworkOutbound;
use std::sync::{mpsc, Mutex, Arc};
use crate::proposal_queue::ProposalQueue;
use std::thread;
use std::time::Duration;

// There are 2 ways to design the communication here
// (1) Directly sending between each node
// By using one receiver for incoming messages on each node, and senders will be clone and giving to all other nodes
// Using Rust mpsc: 1 -> 3 / 2 -> 3
// Advantage: easy to design and implement
// Disadvantage: cannot simulate network error between nodes
// (2) There is a middleware layer: get all messages and redirect to the correct node
// example: 1 -> 3_1 / 2 -> 3_2. Then we collect all messages and forward to the correct node
// Advantage: build a more complex network with rules to emulate the failure in the distributed system
// Disadvantage: harder to implement
// We choose the second solution
pub struct VirtualNetwork {
    n: u64,

    stop_snd: SyncSender<()>,

    // map from node_id -> NetworkInbound (will transfer ownership to caller)
    pub network_inbounds: HashMap<u64, NetworkInbound<ChannelSender<Message>, ChannelSender<Proposal>>>,

    // map from node_id -> NetworkOutbound (will transfer ownership to caller)
    pub network_outbounds: HashMap<u64, NetworkOutbound>,

    pub proposal_queues: HashMap<u64, ProposalQueue>,

    network_core: Arc<Mutex<NetworkCore>>,
}

// all necessary attributes for running moving to NetworkCore which allows to run the network as daemon
struct NetworkCore {
    n: u64,

    stop_rcv: Receiver<()>,

    // map from sender_id -> (receiver_id -> Message) : received from network_inbounds
    mm_receivers: HashMap<u64, HashMap<u64, Receiver<Message>>>,

    // map from sender_id -> (receiver_id -> Proposal): received from network_inbounds
    mm_proposal_receivers: HashMap<u64, HashMap<u64, Receiver<Proposal>>>,

    // map from sender_id -> (receiver_id -> Sender<Message> object)
    // use messages from mm_receivers as  the source then sending messages to network_outbound
    mm_senders: HashMap<u64, HashMap<u64, Sender<Message>>>,

    proposal_queues: HashMap<u64, ProposalQueue>,
}

impl VirtualNetwork {
    pub fn new(n: u64) -> VirtualNetwork {
        let (stop_snd, stop_rcv) = mpsc::sync_channel(0);

        let mut network_inbounds = HashMap::new();
        let mut network_outbounds = HashMap::new();

        let mut mm_senders = HashMap::new();
        let mut mm_receivers = HashMap::new();
        let mut mm_proposal_receivers = HashMap::new();

        let mut proposal_queues = HashMap::new();

        for node in 1..n + 1 {
            mm_receivers.insert(node, HashMap::<u64, Receiver<Message>>::new());
            mm_proposal_receivers.insert(node, HashMap::<u64, Receiver<Proposal>>::new());
            mm_senders.insert(node, HashMap::<u64, Sender<Message>>::new());
            proposal_queues.insert(node, ProposalQueue::new());
        }

        // create network_inbound with the direction from -> all nodes
        for from in 1..n + 1 {
            let mut network_inbound =
                NetworkInbound::<ChannelSender<Message>, ChannelSender<Proposal>>::new();

            for to in 1..n + 1 {
                if from == to {
                    continue;
                }

                let (internal_sender, internal_receiver) =
                    ChannelSender::<Message>::new(from, to);
                let (proposal_sender, proposal_receiver) =
                    ChannelSender::<Proposal>::new(from, to);

                network_inbound.add_conn(internal_sender, proposal_sender);

                mm_receivers.get_mut(&from).unwrap().insert(to, internal_receiver);
                mm_proposal_receivers.get_mut(&from).unwrap().insert(to, proposal_receiver);
            }

            network_inbounds.insert(from, network_inbound);
        }

        // create network outbound with direction all nodes -> to
        for to in 1..n + 1 {
            let (tx, rx) = mpsc::channel::<Message>();
            let mut network_outbound = NetworkOutbound::new(rx);
            network_outbounds.insert(to, network_outbound);

            for from in 1..n + 1 {
                if from == to {
                    continue;
                }
                mm_senders.get_mut(&from).unwrap().insert(to, tx.clone());
            }
        }

        let network_core = Arc::new(Mutex::new(NetworkCore {
            n,
            stop_rcv,
            mm_receivers,
            mm_proposal_receivers,
            mm_senders,
            proposal_queues: proposal_queues.clone(),
        }));

        VirtualNetwork { n, stop_snd, network_core, network_inbounds, network_outbounds, proposal_queues }
    }

    pub fn start(&mut self) {
        self.network_core.lock().unwrap().start();
    }

    pub fn async_start(&mut self) {
        let core = self.network_core.clone();
        thread::spawn(move || {
            core.lock().unwrap().start();
        });
    }

    pub fn stop(&self) {
        self.stop_snd.send(());
    }
}

impl NetworkCore {
    pub fn start(&mut self) {
        loop {
            match self.stop_rcv.try_recv() {
                Ok(_) | Err(TryRecvError::Disconnected) => {
                    println!("Terminate the virtual network");
                    break;
                }
                _ => {}
            }

            for from in 1..self.n + 1 {
                let receivers = self.mm_receivers.get(&from).unwrap();

                // check if [from] send any messages to [to]. then we acted as middle man forward this messages to the [to] node
                for to in 1..self.n + 1 {
                    if from == to {
                        continue;
                    }

                    // println!("virtual_network: try to send from {} to {}", from, to);
                    let receiver = receivers.get(&to).unwrap();
                    match receiver.recv_timeout(Duration::from_millis(10)) {
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            // TODO: cleanup all resource her
                            println!("ERRROR!!!. from {} to {}", from, to);
                            break;
                        }
                        Ok(msg) => {
                            // println!("virtual_network: send from {} to {} with msg [{:?}]", from, to, msg);
                            let sender = self
                                .mm_senders
                                .get_mut(&from).unwrap()
                                .get_mut(&to).unwrap();
                            sender.send(msg);
                        }
                    }
                }

                // send all proposal messages: TODO: we don't have this mechanism yet
                let receivers = self.mm_proposal_receivers.get(&from).unwrap();
                for to in 1..self.n + 1 {
                    if from == to {
                        continue;
                    }

                    // let mut proposal_queue = self.proposal_queues.get_mut(&to).unwrap();
                    // let receiver = receivers.get(&to).unwrap();
                    // match receiver.recv_timeout(Duration::from_millis(10)) {
                    //     Err(RecvTimeoutError::Timeout) => {}
                    //     Err(RecvTimeoutError::Disconnected) => {
                    //         // TODO: cleanup all resource here
                    //         break;
                    //     }
                    //     Ok(proposal) => {
                    //         println!("virtual_network: send from {} to {} with proposal [{:?}]", from, to, proposal);
                    //         proposal_queue.add_proposal(proposal);
                    //     }
                    // }
                }
            }
        }
    }
}