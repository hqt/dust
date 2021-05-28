use command::{Response, QueryRequest, Rows, ExecuteRequest};

#[derive(thiserror::Error, Debug)]
pub enum Error {}

// Database is the interface any queryable system must implement
pub trait Database {
    // Execute executes a slice of queries, each of which is not expected
    // to return rows. If tx is true, then either all queries will be executed
    // successfully or it will as though none executed.
    fn execute(&mut self, req: ExecuteRequest) -> Result<Vec<Response>, Error>;

    // Query executes a slice of queries, each of which returns rows.
    fn query(&self, req: QueryRequest) -> Result<Rows<'static>, Error>;
}

// RaftControl is the interface the Raft-based database must implement.
pub trait RaftControl {
    // join joins the node with the given ID, reachable at addr, to this node.
    fn join(&mut self, id: String, addr: String) -> Result<(), Error>;


    // remove removes the node, specified by id, from the cluster.
    fn remove(&mut self, id: String) -> Result<(), Error>;

    // leader returns the Raft address of the leader of the cluster.
    fn leader_id(&self) -> Result<String, Error>;
}