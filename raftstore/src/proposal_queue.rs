use std::sync::{Mutex, Arc};
use std::collections::VecDeque;
use crate::msg::Proposal;

#[derive(Clone)]
pub struct ProposalQueue {
    proposals: Arc<Mutex<VecDeque::<Proposal>>>,
}

impl ProposalQueue {
    pub fn new() -> Self {
        ProposalQueue { proposals: Arc::new(Mutex::new(Default::default())) }
    }

    pub fn add_proposal(&mut self, proposal: Proposal) {
        self.proposals.lock().unwrap().push_back(proposal);
    }

    pub fn remove_proposal(&mut self) -> Option<Proposal> {
        self.proposals.lock().unwrap().pop_front()
    }

    pub fn get_ref(&mut self) -> Arc<Mutex<VecDeque<Proposal>>> {
        self.proposals.clone()
    }
}
