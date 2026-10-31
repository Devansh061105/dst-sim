//! Deterministic simulator: seeded RNG streams, virtual clock, event queue, main loop and trace recorder.
//!
//! A [`Simulation`] runs a whole cluster of [`sim_api::Node`]s on one thread. Each step pops the
//! earliest event, jumps the virtual clock to it, and hands it to a node; whatever the node asks
//! for (messages, timers) becomes new events. Every random choice comes from streams derived from
//! one seed, so the same seed always produces the same run, fingerprinted by [`Trace::hash`].

mod queue;
mod rng;
mod sim;
pub mod toys;
mod trace;

pub use queue::EventQueue;
pub use rng::{SimRng, Stream};
pub use sim::{RunSummary, SimConfig, Simulation, StopReason};
pub use trace::Trace;
