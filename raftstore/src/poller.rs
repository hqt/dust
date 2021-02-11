use crate::ticker;
use std::time::Duration;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError, Sender, SyncSender};
use crate::peer_fsm::PeerFsm;
use crate::network_outbound::NetworkOutbound;

use raft::{prelude::*};
use std::sync::{Mutex, Arc, mpsc};
use crate::msg::Proposal;
use crate::peer_sender::PeerSender;
use std::collections::VecDeque;
use crate::raft_store::RaftStore;
use raft::storage::MemStorage;
use std::thread;
use crate::proposal_queue::ProposalQueue;
use crate::network_inbound::NetworkInbound;
use crate::peer_fsm_delegate::PeerFsmDelegate;

pub const RAFT_TIMEOUT: Duration = Duration::from_millis(100);

fn default_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    }
}

pub struct Poller {
    stop_snd: SyncSender<()>
}

impl Poller {
    pub fn start<A, B>(
        id: u64,
        proposal_queue: ProposalQueue,
        network_outbound: NetworkInbound<A, B>,
        network_inbound: NetworkOutbound,
        initialize: bool,
    ) -> Self
        where
            A: PeerSender<Message=Message> + Send + 'static,
            B: PeerSender<Message=Proposal> + Send + 'static,
    {
        let (stop_snd, stop_rcv) = mpsc::sync_channel(0);
        let mut raft_ticker = ticker::Ticker::new(RAFT_TIMEOUT);
        let mut timeout = RAFT_TIMEOUT;

        // create peer
        let mut cfg = default_config();
        cfg.id = id;
        let storage = MemStorage::new();
        if initialize {
            let mut s = Snapshot::default();
            s.mut_metadata().index = 1;
            s.mut_metadata().term = 1;
            s.mut_metadata().mut_conf_state().nodes = vec![1];
            storage.wl().apply_snapshot(s).unwrap();
        }

        let raft_group = RawNode::new(&cfg, storage).unwrap();
        let mut fsm = PeerFsm::new(id, raft_group, network_outbound, proposal_queue.clone());
        let mut fsm_delegate = PeerFsmDelegate::new(fsm);

        let pq = proposal_queue.clone();
        thread::spawn(move || {
            loop {
                match stop_rcv.try_recv() {
                    Ok(_) | Err(TryRecvError::Disconnected) => {
                        println!("Terminating node {}", id);
                        break;
                    }
                    _ => {}
                }

                match network_inbound.internal_message_receiver.recv_timeout(timeout) {
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        // TODO: cleanup all resource here
                        break;
                    }
                    Ok(msg) => {
                        fsm_delegate.handle_raft_msg(msg);
                    }
                }

                timeout = raft_ticker.tick(|| {
                    fsm_delegate.tick();
                });

                fsm_delegate.handle_proposals(pq.clone());
                fsm_delegate.on_ready();
            }
        });


        Poller { stop_snd: stop_snd }
    }

    pub fn stop(&self) {
        self.stop_snd.send(());
    }
}
