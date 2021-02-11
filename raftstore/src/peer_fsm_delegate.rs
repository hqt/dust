use raft::{prelude::*, StateRole};

use crate::peer_fsm::PeerFsm;
use crate::peer_sender::PeerSender;
use crate::msg::Proposal;
use crate::proposal_queue::ProposalQueue;

pub struct PeerFsmDelegate<A, B> where
    A: PeerSender<Message=Message>,
    B: PeerSender<Message=Proposal>
{
    fsm: PeerFsm<A, B>
}

impl<A, B> PeerFsmDelegate<A, B> where
    A: PeerSender<Message=Message>,
    B: PeerSender<Message=Proposal>
{
    pub fn new(fsm: PeerFsm<A, B>) -> PeerFsmDelegate<A, B> {
        PeerFsmDelegate { fsm }
    }

    pub fn handle_raft_msg(&mut self, msg: Message) -> Result<(), ()> {
        self.fsm.on_peer_message(msg);
        Ok(())
    }

    pub fn tick(&mut self) {
        self.fsm.raft_group.raft.tick();
    }

    pub fn on_ready(&mut self) {
        self.fsm.on_ready();
    }

    pub fn handle_proposals(&mut self, mut proposal_queue: ProposalQueue) {
        if !self.fsm.is_leader() {
            return;
        }

        let mut fsm = &mut self.fsm;

        for p in proposal_queue.get_ref().lock().unwrap().iter_mut().skip_while(|p| p.proposed > 0) {
            if let Some((ref key, ref value)) = p.normal {
                fsm.on_proposal_normal(p);
            } else if let Some(ref cc) = p.conf_change {
                fsm.on_proposal_cfg_change(p);
            } else if let Some(_transferee) = p.transfer_leader {
                // TODO: implement transfer leader.
                unimplemented!();
            }
        }
    }
}