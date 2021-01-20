use std::sync::mpsc;

use protobuf::Message as PbMessage;
use std::sync::mpsc::{Sender, Receiver};

pub struct Mailbox<T> {
    pub receiver: Receiver<T>,
}

impl<T> Mailbox<T> {
    pub fn new() -> (Mailbox<T>, Sender<T>) {
        let (tx, rx): (Sender<T>, Receiver<T>) = mpsc::channel();
        return (
            Mailbox { receiver: rx },
            tx
        );
    }
}
