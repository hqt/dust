use raftstore::*;
use raft::{prelude::*};

#[test]
fn integration_test() {
    let n = 5;
    let leader_id = 1;

    let mut network = VirtualNetwork::new(n);
    let mut raft_stores = Vec::new();
    let mut pollers = Vec::new();

    for node in 1..n + 1 {
        let mut initialize = false;
        if node == leader_id {
            initialize = true;
        }

        let network_inbound = network.network_inbounds.remove(&node).unwrap();
        let network_outbound = network.network_outbounds.remove(&node).unwrap();
        let proposal_queue = network.proposal_queues.get(&node).unwrap().clone();
        let (raft_store, poller) = start_for_testing(
            node, network_inbound, network_outbound,
            proposal_queue, initialize);
        raft_stores.push(raft_store);
        pollers.push(poller);
    }

    network.async_start();

    for node in 2..n + 1 {
        // send proposal to leader
        let mut conf_change = ConfChange::default();
        conf_change.node_id = node;
        conf_change.set_change_type(ConfChangeType::AddNode);
        let (proposal, rx) = Proposal::conf_change(&conf_change);
        network.proposal_queues.get_mut(&leader_id).unwrap().add_proposal(proposal);

        let res = rx.recv().unwrap();
        assert_eq!(res, true);
        println!("node {} joined successfully", node);
    }

    // send proposals to write message
    for i in 1..10 {
        println!("send message {}", i);
        let (proposal, rx) = Proposal::normal(i, "hello, world".to_owned());
        network.proposal_queues.get_mut(&leader_id).unwrap().add_proposal(proposal);

        // After we got a response from `rx`, we can assume the put succeeded and following
        // `get` operations can find the key-value pair.
        let res = rx.recv().unwrap();
        assert_eq!(res, true);
        println!("send message {} successfully", i);
    }

    println!("done sending messages");

    network.stop();
    for poller in pollers {
        poller.stop();
    }
}