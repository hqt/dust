use std::sync::mpsc::{Receiver, Sender};

use raft::{prelude::*};
use crate::msg::Proposal;

pub struct NetworkOutbound {
    pub internal_message_receiver: Receiver<Message>,
}

impl NetworkOutbound {
    pub fn new(receiver: Receiver<Message>) -> Self {
        NetworkOutbound {
            internal_message_receiver: receiver,
        }
    }
}