//! What an upstream calls a fault, and what the asking leg does about it —
//! two axes one string-matching closure used to answer at once, in seven
//! copies. Neither derives from the other: the same "not found" is an absent
//! block to a receipts fetch and an empty range to a logs fetch.
//!
//! Transport-level classification (rate limits, timeouts, internal errors)
//! stays in `rpc_client::error`: it is not a dialect.

use rpc_client::{RpcError, RpcErrorInfo};
use serde_json::{json, Value};

/// A code, not a name, so it is not a [`Fault`]: some endpoints send it for
/// anything they did not feel like answering.
const INTERNAL_ERROR: i64 = -32000;

/// What the upstream said, in one vocabulary. `None` from [`Fault::of`] means
/// the message lies outside the dialect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The height was rejected outright rather than merely unavailable.
    HeightRejected,
    /// The endpoint will not answer for an unfinalized height.
    Unfinalized,
    /// The block is not held: not produced, or not imported here yet.
    UnknownBlock,
    /// The height is above the tip this endpoint accepts.
    PastAcceptedTip,
}

impl Fault {
    /// Order is the dialect's: a rejected height is a different answer from an
    /// absent block, and `header not found` also contains `not found`.
    pub fn of(info: &RpcErrorInfo) -> Option<Fault> {
        let message = info.message.as_str();
        if message.contains("invalid block height") {
            Some(Fault::HeightRejected)
        } else if message.contains("cannot query unfinalized data") {
            Some(Fault::Unfinalized)
        } else if message.contains("after last accepted block") {
            Some(Fault::PastAcceptedTip)
        } else if message.contains("unknown block") || message.contains("not found") {
            Some(Fault::UnknownBlock)
        } else {
            None
        }
    }
}

/// Which leg is asking. The verdict needs it: legs disagree about what an
/// absent block means for them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leg {
    /// `eth_getBlockByNumber` — the header, speculative or historic.
    Body,
    Logs,
    Receipts,
    /// Any of the `debug_*` / `trace_*` calls.
    Trace,
}

/// What the asking leg does about a fault.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Recovery {
    /// The fault *is* the answer: absorb it as this value.
    Absorb(Value),
    /// Ask again, unchanged, through the client's own retry ladder.
    Retry,
    /// Ask again — of a *different* backend: receipts cannot be late on the
    /// node that showed the header, so a backend lacking the block is a
    /// statement about who answered.
    ///
    /// What executes it is deliberately weak. It surfaces as an absent answer,
    /// so the ladder waits and only its whole-block escalation redraws. Moving
    /// sooner costs a round trip per lagging block — its own decision.
    Elsewhere,
    /// Not ours to heal.
    Surface,
}

/// The verdict for one leg and one upstream error.
pub fn recovery(leg: Leg, info: &RpcErrorInfo) -> Recovery {
    match (leg, Fault::of(info)) {
        (Leg::Body, Some(Fault::Unfinalized)) => Recovery::Absorb(Value::Null),
        (Leg::Body, Some(Fault::HeightRejected)) => Recovery::Retry,
        // After the two rules above, so a named fault sent under this code
        // keeps the named verdict.
        (Leg::Body, _) if info.code == INTERNAL_ERROR => Recovery::Retry,

        (Leg::Logs, Some(Fault::PastAcceptedTip)) => Recovery::Absorb(json!([])),

        (Leg::Receipts, Some(Fault::HeightRejected)) => Recovery::Retry,
        (Leg::Receipts, Some(Fault::UnknownBlock)) => Recovery::Elsewhere,

        (Leg::Trace, Some(Fault::UnknownBlock | Fault::Unfinalized)) => {
            Recovery::Absorb(Value::Null)
        }

        _ => Recovery::Surface,
    }
}

/// The `validate_error` hook for one leg.
pub fn validator(leg: Leg) -> Box<dyn Fn(&RpcErrorInfo) -> Result<Value, RpcError> + Send + Sync> {
    Box::new(move |info: &RpcErrorInfo| match recovery(leg, info) {
        Recovery::Absorb(value) => Ok(value),
        // This hook cannot address a backend; the ladder can.
        Recovery::Elsewhere => Ok(Value::Null),
        Recovery::Retry => Err(RpcError::RetryRequested(info.message.clone())),
        Recovery::Surface => Err(RpcError::Rpc {
            code: info.code,
            message: info.message.clone(),
            data: info.data.clone(),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(code: i64, message: &str) -> RpcErrorInfo {
        RpcErrorInfo {
            code,
            message: message.to_string(),
            data: None,
        }
    }

    /// The dialect, as one vocabulary rather than five closures' worth of
    /// substring checks.
    #[test]
    fn every_dialect_term_classifies() {
        for (message, fault) in [
            ("invalid block height 99", Fault::HeightRejected),
            ("cannot query unfinalized data", Fault::Unfinalized),
            (
                "cannot query block after last accepted block",
                Fault::PastAcceptedTip,
            ),
            ("unknown block", Fault::UnknownBlock),
            ("not found", Fault::UnknownBlock),
            ("header not found", Fault::UnknownBlock),
        ] {
            assert_eq!(Fault::of(&info(1, message)), Some(fault), "{message}");
        }
        assert_eq!(Fault::of(&info(-32000, "boom")), None);
    }

    /// Equivalence against the table the closures implemented, term by term
    /// and leg by leg. The one deliberate change is noted below.
    #[test]
    fn the_table_matches_the_closures_it_replaced() {
        let absorb_null = Recovery::Absorb(Value::Null);
        let absorb_empty = Recovery::Absorb(json!([]));

        // eth_getBlockByNumber
        assert_eq!(
            recovery(Leg::Body, &info(1, "cannot query unfinalized data")),
            absorb_null
        );
        assert_eq!(
            recovery(Leg::Body, &info(1, "invalid block height")),
            Recovery::Retry
        );
        assert_eq!(
            recovery(Leg::Body, &info(INTERNAL_ERROR, "boom")),
            Recovery::Retry
        );
        assert_eq!(
            recovery(Leg::Body, &info(1, "not found")),
            Recovery::Surface
        );

        // eth_getLogs
        assert_eq!(
            recovery(Leg::Logs, &info(1, "requested after last accepted block")),
            absorb_empty
        );
        assert_eq!(
            recovery(Leg::Logs, &info(1, "not found")),
            Recovery::Surface
        );
        assert_eq!(
            recovery(Leg::Logs, &info(INTERNAL_ERROR, "boom")),
            Recovery::Surface
        );

        // eth_getBlockReceipts
        assert_eq!(
            recovery(Leg::Receipts, &info(1, "invalid block height")),
            Recovery::Retry
        );
        for message in ["unknown block", "not found", "header not found"] {
            assert_eq!(
                recovery(Leg::Receipts, &info(1, message)),
                Recovery::Elsewhere,
                "{message}"
            );
        }
        assert_eq!(
            recovery(Leg::Receipts, &info(INTERNAL_ERROR, "boom")),
            Recovery::Surface
        );

        // debug_trace* / trace_*
        for message in [
            "not found",
            "header not found",
            "cannot query unfinalized data",
        ] {
            assert_eq!(
                recovery(Leg::Trace, &info(1, message)),
                absorb_null,
                "{message}"
            );
        }
        assert_eq!(
            recovery(Leg::Trace, &info(1, "invalid block height")),
            Recovery::Surface
        );
        assert_eq!(
            recovery(Leg::Trace, &info(INTERNAL_ERROR, "boom")),
            Recovery::Surface
        );

        // The one widening: the trace legs matched only `not found`, so a bare
        // `unknown block` used to end the session. It is now the same absent
        // block the receipts leg has always recognised, and reaches the same
        // bounded re-acquisition budget.
        assert_eq!(recovery(Leg::Trace, &info(1, "unknown block")), absorb_null);
    }

    #[test]
    fn a_named_fault_outranks_a_bare_internal_code() {
        assert_eq!(
            recovery(
                Leg::Body,
                &info(INTERNAL_ERROR, "cannot query unfinalized data")
            ),
            Recovery::Absorb(Value::Null)
        );
    }
}
