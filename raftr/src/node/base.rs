use crate::configuration::Configuration;
use crate::message::Message;
use crate::node::candidate;
use crate::node::follower;
use crate::node::leader;
use crate::node::Node;
use crate::pb;
use crate::peer::Peer;
use crate::types::{NodeId, Term};
use bytes::Bytes;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::error;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct BaseNode<P: Peer + Clone + Send + Sync + 'static> {
    id: NodeId,
    conf: Arc<Configuration>,

    // TODO: make these persistent
    term: Term,
    voted_for: Option<NodeId>,

    node: Node,

    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    peers: HashMap<NodeId, P>,
}

impl<P: Peer + Clone + Send + Sync + 'static> BaseNode<P> {
    pub fn new(id: NodeId, conf: Configuration) -> Self {
        let (tx, rx) = mpsc::channel(100);
        BaseNode {
            id,
            conf: Arc::new(conf),
            term: 1,
            voted_for: None,
            node: Node::new(),
            tx,
            rx,
            peers: HashMap::new(),
        }
    }

    async fn handle_messages(&mut self) -> Result<(), Box<dyn error::Error + Send>> {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Message::RPCRequestVoteRequest { req, tx } => {
                    self.handle_request_vote_request(req, tx).await
                }

                Message::RPCRequestVoteResponse { res, id } => {
                    self.handle_request_vote_response(res, id).await
                }

                Message::RPCAppendEntriesRequest { req, tx } => {
                    self.handle_append_entries_request(req, tx).await
                }

                Message::ElectionTimeout => self.handle_election_timeout().await,

                Message::Command { body, tx } => self.handle_command(body, tx).await?,
                _ => {}
            }
        }
        Ok(())
    }

    // Update the node's term and return to Follower state,
    // if the term of the request is newer than the node's current term.
    fn update_term(&mut self, req_id: NodeId, req_term: Term) {
        if req_term > self.term {
            debug!(
                "id={}, term={}, message=\"receive a request from {} which has higher term: {}\"",
                self.id, self.term, req_id, req_term,
            );
            self.term = req_term;
            self.voted_for = None;
            if let Node::Follower { .. } = self.node {
                self.trans_state_follower();
            }
        }
    }

    async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
        mut tx: mpsc::Sender<Result<pb::RequestVoteResponse, Box<dyn error::Error + Send>>>,
    ) {
        self.update_term(req.candidate_id, req.term);
        let res = self.node.handle_request_vote_request(req).await;

        tokio::spawn(async move {
            let r = tx.send(res).await;

            if let Err(e) = r {
                warn!("{}", e);
            }
        });
    }

    async fn handle_request_vote_response(&mut self, res: pb::RequestVoteResponse, id: NodeId) {
        if self.node.handle_request_vote_response(res, id).await {
            self.trans_state_leader();
        }
    }

    async fn handle_append_entries_request(
        &mut self,
        req: pb::AppendEntriesRequest,
        mut tx: mpsc::Sender<Result<pb::AppendEntriesResponse, Box<dyn error::Error + Send>>>,
    ) {
        self.update_term(req.leader_id, req.term);
        let res = self.node.handle_append_entries_request(req).await;

        tokio::spawn(async move {
            let r = tx.send(res).await;

            if let Err(e) = r {
                warn!("{}", e);
            }
        });
    }

    async fn handle_election_timeout(&mut self) {
        // TODO: handle the case where the node is already a leader.
        self.term += 1;
        self.trans_state_candidate();

        self.node.handle_election_timeout(&self.peers).await;
    }

    async fn handle_command(
        &mut self,
        command: Bytes,
        tx: mpsc::Sender<Result<(), tonic::Status>>,
    ) -> Result<(), Box<dyn error::Error + Send>> {
        if let Node::Leader { ref leader } = self.node {}
        Ok(())
    }

    fn trans_state_follower(&mut self) {
        info!(
            "id={}, term={}, state={}, message=\"{}\"",
            self.id,
            self.term,
            self.node.to_ident(),
            "become a follower."
        );

        self.node = Node::Follower {
            follower: follower::Follower::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                self.node.log(),
                self.tx.clone(),
            ),
        };
    }

    fn trans_state_candidate(&mut self) {
        info!(
            "id={}, term={}, state={}, message=\"{}\"",
            self.id,
            self.term,
            self.node.to_ident(),
            "become a candidate."
        );

        let quorum = (self.peers.len() + 1) / 2;
        self.node = Node::Candidate {
            candidate: candidate::Candidate::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                quorum,
                self.node.log(),
                self.tx.clone(),
            ),
        };
        self.voted_for = Some(self.id);
    }

    fn trans_state_leader(&mut self) {
        info!(
            "id={}, term={}, state={}, message=\"{}\"",
            self.id,
            self.term,
            self.node.to_ident(),
            "become a leader."
        );

        self.node = Node::Leader {
            leader: leader::Leader::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                self.node.log(),
                &self.peers,
            ),
        };
    }

    pub async fn run(
        mut self,
        peers: HashMap<NodeId, P>,
    ) -> Result<(), Box<dyn error::Error + Send>> {
        self.peers = peers;
        self.trans_state_follower();

        self.handle_messages().await
    }

    pub fn tx(&self) -> mpsc::Sender<Message> {
        self.tx.clone()
    }
}
