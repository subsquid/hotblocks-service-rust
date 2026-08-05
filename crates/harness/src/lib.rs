//! Conformance harness for the hotblocks service — Phase 0 of the build order
//! in `spec/13-conformance-tdd.md`.
//!
//! - [`model`] — the executable reference model (HC-5): the `spec/13`
//!   pseudocode as code. The oracle for every CT-1 comparison.
//! - [`sim`] — deterministic, seedable input simulator (HC-1) with a
//!   provenance ledger (HC-2): every block and finality report it delivers is
//!   recorded with its complete model-relevant linkage tuple, so tests can
//!   replay supported one-session ingestion histories into the model
//!   (`Ledger::replay`) and check served bytes against produced bytes.
//! - [`script`] — a scripted adapter-level source (HC-3) for the pathological
//!   histories the simulator does not generate, driven through SUT and model
//!   alike.
//! - [`upstream`] — the fault-injecting JSON-RPC upstream (HC-3) the real
//!   acquisition adapter is driven against.
//! - [`validators`] — kind-agnostic structural validators (HC-6) applied to
//!   every response.
//! - [`client`] — a minimal HTTP driver for the service's wire surface.

pub mod client;
pub mod model;
pub mod script;
pub mod sim;
pub mod upstream;
pub mod validators;
