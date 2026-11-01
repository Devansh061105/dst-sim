//! One protocol written twice: a server that echoes requests, and clients that send numbered
//! requests one at a time and retry each until it is answered.
//!
//! [`Echo`] is the callback version, a state machine. [`async_echo`] is the same protocol as an
//! `async` function. They make the same sends and timer calls in the same order, so for any seed
//! the two runs end at the same virtual time after the same number of steps, with the same
//! message statistics (their trace hashes differ only because the timer payloads have different
//! types).

use std::time::Duration;

use sim_api::{Context, Node, NodeId, TimerId};

use crate::AsyncNode;

/// The server is node 0; every other node is a client.
pub const SERVER: NodeId = NodeId(0);

/// How long a client waits for a reply before sending the request again.
pub const RETRY: Duration = Duration::from_millis(100);

/// Echo protocol messages.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EchoMsg {
    /// Request number `n`.
    Request(u32),
    /// The answer to request `n`.
    Reply(u32),
}

/// Timer payload of the callback version.
#[derive(Clone, Debug, Hash)]
pub struct Retry;

/// The callback version.
#[derive(Debug)]
pub enum Echo {
    /// Answers every request.
    Server,
    /// Sends `total` requests in order; `next` is the one waiting for a reply.
    Client {
        /// Requests to make in all.
        total: u32,
        /// The request in flight (`total` when done).
        next: u32,
        /// The retry timer of the request in flight.
        retry: Option<TimerId>,
    },
}

impl Echo {
    /// Node `id` of an echo cluster whose clients make `requests` requests each.
    pub fn new(id: NodeId, requests: u32) -> Self {
        if id == SERVER {
            Echo::Server
        } else {
            Echo::Client { total: requests, next: 0, retry: None }
        }
    }

    /// Whether this is a client with every request answered (the server is never done).
    pub fn is_done(&self) -> bool {
        matches!(self, Echo::Client { total, next, .. } if next == total)
    }

    fn send_next(ctx: &mut dyn Context<EchoMsg, Retry>, next: u32, retry: &mut Option<TimerId>) {
        ctx.send(SERVER, EchoMsg::Request(next));
        *retry = Some(ctx.set_timer(RETRY, Retry));
    }
}

impl Node for Echo {
    type Message = EchoMsg;
    type Timer = Retry;

    fn on_start(&mut self, ctx: &mut dyn Context<EchoMsg, Retry>) {
        if let Echo::Client { total, next, retry } = self {
            *next = 0;
            if *total > 0 {
                Self::send_next(ctx, 0, retry);
            }
        }
    }

    fn on_message(&mut self, ctx: &mut dyn Context<EchoMsg, Retry>, from: NodeId, msg: EchoMsg) {
        match (self, msg) {
            (Echo::Server, EchoMsg::Request(n)) => ctx.send(from, EchoMsg::Reply(n)),
            (Echo::Client { total, next, retry }, EchoMsg::Reply(n)) if n == *next => {
                if let Some(id) = retry.take() {
                    ctx.cancel_timer(id);
                }
                *next += 1;
                if *next < *total {
                    Self::send_next(ctx, *next, retry);
                }
            }
            // A duplicate or late reply: keep waiting on the same deadline.
            _ => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Context<EchoMsg, Retry>, id: TimerId, _: Retry) {
        if let Echo::Client { next, retry, .. } = self
            && *retry == Some(id)
        {
            Self::send_next(ctx, *next, retry);
        }
    }
}

/// The async version of [`Echo`].
pub fn async_echo(id: NodeId, requests: u32) -> AsyncNode<EchoMsg> {
    if id == SERVER {
        return AsyncNode::new(|io| async move {
            loop {
                let (from, msg) = io.recv().await;
                if let EchoMsg::Request(n) = msg {
                    io.send(from, EchoMsg::Reply(n));
                }
            }
        });
    }
    AsyncNode::new(move |io| async move {
        for n in 0..requests {
            io.send(SERVER, EchoMsg::Request(n));
            let mut retry = io.start_timer(RETRY);
            loop {
                match io.recv_or(&retry).await {
                    Some((_, EchoMsg::Reply(m))) if m == n => {
                        io.cancel(retry);
                        break;
                    }
                    // A duplicate or late reply: keep waiting on the same deadline.
                    Some(_) => {}
                    None => {
                        io.send(SERVER, EchoMsg::Request(n));
                        retry = io.start_timer(RETRY);
                    }
                }
            }
        }
    })
}
