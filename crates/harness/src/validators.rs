//! Kind-agnostic structural validators (HC-6) — spec 13 §Structural validators.
//!
//! Applied to every response regardless of test class: decodable → records
//! self-delimiting → ascending → linked → coverage starts at the lowest
//! response-eligible height ≥ the requested one → records stay on the eligible
//! branch → no duplicates → conflict bodies non-empty/ascending → bodies match
//! their binding.

use crate::model::ModelBlock;
use crate::sim::Ledger;
use data_service_core::types::BlockRef;
use flate2::read::MultiGzDecoder;
use std::io::Read;

/// Extracts (number, parentNumber, hash, parentHash) from one decoded payload
/// record. Chain-family specific; the simulator's payload carries them at top
/// level.
pub type Extract = fn(&serde_json::Value) -> Option<(u64, u64, String, String)>;

/// Extractor for the simulator payload format.
pub fn sim_extract(v: &serde_json::Value) -> Option<(u64, u64, String, String)> {
    Some((
        v.get("number")?.as_u64()?,
        v.get("parentNumber")?.as_u64()?,
        v.get("hash")?.as_str()?.to_string(),
        v.get("parentHash")?.as_str()?.to_string(),
    ))
}

/// One independently decodable unit of a response body: a zstd frame or a
/// gzip member (REQ-6, IB-2).
#[derive(Debug, Clone)]
pub struct Frame {
    /// Byte length of the frame within the body — the cut point a client may
    /// truncate at.
    pub bytes: usize,
    pub text: String,
}

/// Split a body into its frames, decoding each **on its own**. A body that is
/// one frame carrying several records yields one `Frame` with several lines,
/// which is what `validate_framing` rejects: whole-stream decoding cannot tell
/// the two apart, so it is not evidence of REQ-6 framing.
pub fn split_frames(body: &[u8], content_encoding: &str) -> anyhow::Result<Vec<Frame>> {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < body.len() {
        let (consumed, out) = match content_encoding {
            "zstd" => decode_one_zstd_frame(&body[pos..])?,
            "gzip" => decode_one_gzip_member(&body[pos..])?,
            other => anyhow::bail!("unexpected content-encoding: {other}"),
        };
        anyhow::ensure!(consumed > 0, "frame consumed no input at byte {pos}");
        pos += consumed;
        frames.push(Frame {
            bytes: consumed,
            text: String::from_utf8(out)?,
        });
    }
    Ok(frames)
}

/// Decode exactly one zstd frame, returning (bytes consumed, output).
fn decode_one_zstd_frame(input: &[u8]) -> anyhow::Result<(usize, Vec<u8>)> {
    use zstd::stream::raw::{Decoder, InBuffer, Operation, OutBuffer};

    let mut decoder = Decoder::new()?;
    let mut in_buf = InBuffer::around(input);
    let mut scratch = vec![0u8; 64 * 1024];
    let mut out = Vec::new();
    loop {
        let mut out_buf = OutBuffer::around(&mut scratch[..]);
        let hint = decoder.run(&mut in_buf, &mut out_buf)?;
        let produced = out_buf.pos();
        out.extend_from_slice(&scratch[..produced]);
        if hint == 0 {
            break; // frame complete — anything after it is the next frame
        }
        anyhow::ensure!(
            in_buf.pos < in_buf.src.len() || produced > 0,
            "truncated zstd frame"
        );
    }
    Ok((in_buf.pos, out))
}

/// Decode exactly one gzip member, returning (bytes consumed, output).
///
/// The header is parsed here (RFC 1952) and the payload inflated raw, because
/// flate2's gzip-aware `Decompress` needs a zlib backend this build does not
/// have — and a member-crossing decoder cannot report a boundary anyway.
fn decode_one_gzip_member(input: &[u8]) -> anyhow::Result<(usize, Vec<u8>)> {
    use flate2::{Decompress, FlushDecompress, Status};

    let header = gzip_header_len(input)?;
    let mut dec = Decompress::new(false);
    let mut out = Vec::new();
    let mut scratch = vec![0u8; 64 * 1024];
    loop {
        let consumed = header + dec.total_in() as usize;
        anyhow::ensure!(consumed <= input.len(), "truncated gzip member");
        let before_out = dec.total_out();
        let status = dec.decompress(&input[consumed..], &mut scratch, FlushDecompress::None)?;
        let produced = (dec.total_out() - before_out) as usize;
        out.extend_from_slice(&scratch[..produced]);
        match status {
            Status::StreamEnd => break,
            _ => anyhow::ensure!(
                header + dec.total_in() as usize > consumed || produced > 0,
                "gzip member made no progress"
            ),
        }
    }
    // CRC32 + ISIZE close the member.
    let total = header + dec.total_in() as usize + 8;
    anyhow::ensure!(total <= input.len(), "gzip member lacks its trailer");
    Ok((total, out))
}

/// Length of one gzip member's header (RFC 1952 §2.3).
fn gzip_header_len(input: &[u8]) -> anyhow::Result<usize> {
    anyhow::ensure!(input.len() >= 10, "gzip member shorter than its header");
    anyhow::ensure!(
        input[0] == 0x1f && input[1] == 0x8b && input[2] == 8,
        "not a deflate gzip member"
    );
    let flags = input[3];
    let mut pos = 10;
    if flags & 0x04 != 0 {
        anyhow::ensure!(input.len() >= pos + 2, "truncated FEXTRA");
        let xlen = u16::from_le_bytes([input[pos], input[pos + 1]]) as usize;
        pos += 2 + xlen;
    }
    for flag in [0x08, 0x10] {
        if flags & flag != 0 {
            let end = input[pos..]
                .iter()
                .position(|b| *b == 0)
                .ok_or_else(|| anyhow::anyhow!("unterminated gzip header string"))?;
            pos += end + 1;
        }
    }
    if flags & 0x02 != 0 {
        pos += 2;
    }
    anyhow::ensure!(pos <= input.len(), "truncated gzip header");
    Ok(pos)
}

/// REQ-6/IB-2: the body is a concatenation of independent per-block frames,
/// each carrying exactly one record, and every frame boundary is a safe cut —
/// the prefix ending there decodes to exactly the records before it.
pub fn validate_framing(body: &[u8], content_encoding: &str) -> Result<Vec<String>, String> {
    let enc = if content_encoding == "identity" {
        "gzip"
    } else {
        content_encoding
    };
    let frames = split_frames(body, enc).map_err(|e| format!("REQ-6: {e}"))?;
    if frames.is_empty() {
        return Err("REQ-6: successful body carries no frame".into());
    }
    for (i, f) in frames.iter().enumerate() {
        if !f.text.ends_with('\n') {
            return Err(format!("REQ-6: frame {i} is not newline-terminated"));
        }
        if f.text.trim_end_matches('\n').contains('\n') {
            return Err(format!(
                "REQ-6: frame {i} carries {} records — frames are per block",
                f.text.matches('\n').count()
            ));
        }
    }
    // Truncation safety (ADR-10): cutting at any frame boundary leaves every
    // delivered record valid and complete.
    let mut cut = 0;
    for (i, f) in frames.iter().enumerate() {
        cut += f.bytes;
        let prefix = split_frames(&body[..cut], enc)
            .map_err(|e| format!("REQ-6: prefix through frame {i} does not decode: {e}"))?;
        if prefix.len() != i + 1 {
            return Err(format!(
                "REQ-6: prefix through frame {i} decoded {} records, expected {}",
                prefix.len(),
                i + 1
            ));
        }
    }
    Ok(frames.into_iter().map(|f| f.text).collect())
}

/// Decode a stream body per its content-encoding into the concatenated
/// payload text. Both encodings are per-block frames/members (REQ-6).
pub fn decode_body(body: &[u8], content_encoding: &str) -> anyhow::Result<String> {
    let mut out = Vec::new();
    match content_encoding {
        "zstd" => {
            // Concatenated zstd frames: the streaming decoder crosses frames.
            zstd::stream::read::Decoder::new(body)?.read_to_end(&mut out)?;
        }
        "gzip" => {
            // Concatenated gzip members.
            MultiGzDecoder::new(body).read_to_end(&mut out)?;
        }
        other => anyhow::bail!("unexpected content-encoding: {other}"),
    }
    Ok(String::from_utf8(out)?)
}

/// One validated record.
#[derive(Debug, Clone)]
pub struct RecordRef {
    pub number: u64,
    pub parent_number: u64,
    pub hash: String,
    pub parent_hash: String,
}

/// Oracles a DATA response is judged against. Both are optional: without them
/// only self-contained structure (linkage, ordering, boundaries) is checked.
#[derive(Default, Clone, Copy)]
pub struct DataOracle<'a> {
    /// Byte provenance (INV-25): what the simulator actually produced for a
    /// given `(number, hash, parentNumber, parentHash)` linkage tuple. Says
    /// nothing about which heights a response may start at — the ledger is
    /// cumulative, so it also holds orphaned and evicted blocks.
    pub ledger: Option<&'a Ledger>,
    /// Every block the response is allowed to draw from, ascending: for a
    /// window-underflow query the backfill prefix RP-8 owes, spliced onto the
    /// snapshot; otherwise the snapshot alone. Branch- and eviction-aware, so
    /// it is the only sound oracle for the lowest response-eligible height
    /// (RP-5/DEF-30).
    ///
    /// Valid only if the response cannot have overtaken it — under quiescence,
    /// or with racing commits (free variable 4) accounted for by the caller.
    pub eligible: Option<&'a [ModelBlock]>,
}

/// Validate a decoded DATA body (INV-20, INV-23, INV-25, RP-5).
///
/// Checks: newline-terminated self-delimiting JSON records; non-empty
/// (progress guarantee); never starting below `from`; strictly ascending;
/// pairwise linked per DEF-5 (parent number AND parent hash); no duplicates.
/// With an eligible chain: coverage starts at its lowest height ≥ `from`
/// (DEF-30 — heights may be skipped, so exact equality with `from` is a
/// per-chain-family assertion the caller adds) and every record inside its
/// range matches it on hash *and* parentRef, which is what rules out an
/// abandoned branch and a forged parent on the first record, where pairwise
/// linkage has nothing to compare against. With a ledger: each record's bytes
/// equal the bytes the simulator produced for that complete linkage tuple.
pub fn validate_data(
    text: &str,
    from: u64,
    extract: Extract,
    oracle: &DataOracle,
) -> Result<Vec<RecordRef>, String> {
    if text.is_empty() {
        return Err("INV-23: empty body on a 200 response".into());
    }
    if !text.ends_with('\n') {
        return Err("REQ-6: body does not end at a record boundary".into());
    }
    let mut records: Vec<RecordRef> = vec![];
    for line in text.split_inclusive('\n') {
        let v: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("record {} not valid JSON: {e}", records.len()))?;
        let (number, parent_number, hash, parent_hash) =
            extract(&v).ok_or_else(|| format!("record {} lacks linkage fields", records.len()))?;
        if let Some(prev) = records.last() {
            if number <= prev.number {
                return Err(format!(
                    "INV-20: records not strictly ascending at {number} after {}",
                    prev.number
                ));
            }
            if parent_number != prev.number || parent_hash != prev.hash {
                return Err(format!(
                    "INV-20/DEF-5: record {number} not linked to {}",
                    prev.number
                ));
            }
        } else {
            if number < from {
                return Err(format!(
                    "INV-20: coverage starts at {number}, below requested {from}"
                ));
            }
            if let Some(chain) = oracle.eligible {
                match chain.iter().find(|b| b.number >= from) {
                    Some(lowest) if lowest.number != number => {
                        return Err(format!(
                            "RP-5/DEF-30: coverage starts at {number}, but {} is the \
lowest eligible height ≥ {from}",
                            lowest.number
                        ));
                    }
                    None => {
                        return Err(format!(
                            "RP-5: DATA from {number}, but no eligible height ≥ {from} \
exists — the empty form was owed"
                        ));
                    }
                    _ => {}
                }
            }
        }
        // Above the eligible range a racing commit is allowed (free variable
        // 4), so those records are only structure-checked.
        if let Some(chain) = oracle.eligible.filter(|c| !c.is_empty()) {
            let in_range = number >= chain[0].number && number <= chain[chain.len() - 1].number;
            match chain.iter().find(|b| b.number == number) {
                Some(b) if b.hash != hash => {
                    return Err(format!(
                        "RP-5/INV-21: record {number} is {hash}, off the eligible \
branch (expected {})",
                        b.hash
                    ));
                }
                Some(b) if b.parent_number != parent_number || b.parent_hash != parent_hash => {
                    return Err(format!(
                        "DEF-5: record {number} claims parent ({parent_number}, \
{parent_hash}), not ({}, {})",
                        b.parent_number, b.parent_hash
                    ));
                }
                None if in_range => {
                    return Err(format!(
                        "RP-5: record {number} sits inside the eligible range but at \
no eligible height"
                    ));
                }
                _ => {}
            }
        }
        if let Some(l) = oracle.ledger {
            let header = ModelBlock {
                number,
                hash: hash.clone(),
                parent_number,
                parent_hash: parent_hash.clone(),
            };
            match l.line_of(&header) {
                Some(expected) if expected == line => {}
                Some(_) => return Err(format!("INV-25: served bytes differ for block {number}")),
                None => return Err(format!("INV-25: block {number} was never produced")),
            }
        }
        records.push(RecordRef {
            number,
            parent_number,
            hash,
            parent_hash,
        });
    }
    Ok(records)
}

/// `P-FORK-REFS-MAX`: the conflict-ref ceiling RP-7 bounds `prev` by. The
/// window path emits an inclusive span of 101 refs, the head path up to 100;
/// the contract is the larger, and which count a response picks inside it is a
/// declared free variable (13 §free-variables).
pub const MAX_FORK_REFS: usize = 101;

/// Validate a conflict body (INV-22 shape: non-empty, ascending, bounded).
pub fn validate_conflict(body: &serde_json::Value) -> Result<Vec<BlockRef>, String> {
    let arr = body
        .get("previousBlocks")
        .and_then(|v| v.as_array())
        .ok_or("INV-22: conflict body lacks previousBlocks")?;
    if arr.is_empty() {
        return Err("INV-22: empty previousBlocks (GAP-6)".into());
    }
    if arr.len() > MAX_FORK_REFS {
        return Err(format!(
            "RP-7: {} refs exceed P-FORK-REFS-MAX ({MAX_FORK_REFS})",
            arr.len()
        ));
    }
    let mut refs = vec![];
    for v in arr {
        refs.push(BlockRef {
            number: v
                .get("number")
                .and_then(|n| n.as_u64())
                .ok_or("INV-22: ref lacks number")?,
            hash: v
                .get("hash")
                .and_then(|h| h.as_str())
                .ok_or("INV-22: ref lacks hash")?
                .to_string(),
        });
    }
    for w in refs.windows(2) {
        if w[0].number >= w[1].number {
            return Err("INV-22: previousBlocks not ascending".into());
        }
    }
    Ok(refs)
}

/// The response headers the validators judge (IB-4..IB-6). Watermark headers
/// are carried raw: their presence rules differ per status — the empty form
/// must have them (IB-5), a conflict must not (IB-6).
#[derive(Debug, Default, Clone, Copy)]
pub struct WireHeaders<'a> {
    pub content_type: Option<&'a str>,
    pub content_encoding: Option<&'a str>,
    pub vary: Option<&'a str>,
    pub finalized_number: Option<&'a str>,
    pub finalized_hash: Option<&'a str>,
}

/// Validate the binding headers on a non-empty `/stream` response (IB-1,
/// IB-2, IB-4), returning its parsed finalized watermark.
pub fn validate_data_headers(
    headers: &WireHeaders,
    accept_encoding: &str,
) -> Result<BlockRef, String> {
    expect_text(headers.content_type, 200)?;

    let expected_encoding = if accept_encoding.contains("zstd") {
        "zstd"
    } else {
        "gzip"
    };
    match headers.content_encoding {
        Some(actual) if actual == expected_encoding => {}
        Some(actual) => {
            return Err(format!(
                "IB-2: negotiated {expected_encoding}, response uses {actual}"
            ));
        }
        None => return Err("IB-2: DATA response lacks Content-Encoding".into()),
    }

    let varies_on_encoding = headers.vary.is_some_and(|value| {
        value
            .split(',')
            .any(|token| token.trim().eq_ignore_ascii_case("accept-encoding"))
    });
    if !varies_on_encoding {
        return Err("IB-2: DATA response does not vary on Accept-Encoding".into());
    }

    validate_watermark(headers.finalized_number, headers.finalized_hash)
}

/// The closed error taxonomy (RP-13), as bound to statuses in IB-7.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    InvalidRequest,
    Conflict,
    Empty,
    NotFound,
    Internal,
}

/// Any non-DATA outcome of any endpoint. The taxonomy of RP-13 covers the
/// query surface; 14's operation table binds two more statuses that belong to
/// no RP-13 class — they are transport-level, not query verdicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// An RP-13 class (IB-5..IB-7).
    Class(ErrorClass),
    /// Wrong method on a known route (IB-1).
    MethodNotAllowed,
    /// Readiness negative or probe failure (14's operation table, RP-10).
    Unavailable,
}

/// Validate any response's non-DATA outcome — the validator library's
/// every-response entry point. `/stream` outcomes must additionally be RP-13
/// classes; use [`validate_error`] there.
pub fn validate_outcome(
    status: u16,
    headers: &WireHeaders,
    body: &[u8],
) -> Result<Outcome, String> {
    match status {
        // IB-1 mandates the status; no body is prescribed.
        405 => Ok(Outcome::MethodNotAllowed),
        503 => {
            expect_text(headers.content_type, status)?;
            if body != b"false" {
                return Err(format!(
                    "14 §operations: readiness 503 body is {:?}, not `false`",
                    String::from_utf8_lossy(body)
                ));
            }
            Ok(Outcome::Unavailable)
        }
        _ => validate_error(status, headers, body).map(Outcome::Class),
    }
}

/// IB-1: text bodies carry a `text/plain` content type.
fn expect_text(content_type: Option<&str>, status: u16) -> Result<(), String> {
    match content_type {
        Some(ct) if is_media_type(ct, "text/plain") => Ok(()),
        Some(ct) => Err(format!("IB-1: {status} body is {ct}, not text/plain")),
        None => Err(format!("IB-1: {status} text body lacks Content-Type")),
    }
}

fn is_media_type(value: &str, expected: &str) -> bool {
    value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim()
        .eq_ignore_ascii_case(expected)
}

/// Validate a `/stream` non-DATA outcome against the closed taxonomy
/// (INV-27): a recognised status and the body shape that class mandates
/// (IB-5..IB-7). A status outside RP-13 — including the 405 and 503 of 14's
/// operation table — is a breach *on this surface*, which is why the transport
/// outcomes live in [`validate_outcome`] instead.
pub fn validate_error(
    status: u16,
    headers: &WireHeaders,
    body: &[u8],
) -> Result<ErrorClass, String> {
    match status {
        204 => {
            if !body.is_empty() {
                return Err("IB-5: empty form carries a body".into());
            }
            // IB-5: the finalized-head headers are present on the empty form —
            // without them the client cannot re-poll against a watermark.
            validate_watermark(headers.finalized_number, headers.finalized_hash)
                .map_err(|e| format!("IB-5: empty form lacks watermarks: {e}"))?;
            Ok(ErrorClass::Empty)
        }
        400 | 404 => {
            expect_text(headers.content_type, status)?;
            if body.is_empty() {
                return Err(format!("IB-7: {status} carries no text diagnostic"));
            }
            std::str::from_utf8(body)
                .map_err(|e| format!("IB-7: {status} diagnostic is not text: {e}"))?;
            Ok(if status == 400 {
                ErrorClass::InvalidRequest
            } else {
                ErrorClass::NotFound
            })
        }
        409 => {
            match headers.content_type {
                Some(ct) if is_media_type(ct, "application/json") => {}
                Some(_) => return Err("IB-6: conflict body is not application/json".into()),
                None => return Err("IB-6: conflict body lacks Content-Type".into()),
            }
            if headers.finalized_number.is_some() || headers.finalized_hash.is_some() {
                return Err("IB-6: conflict carries finalized-head headers".into());
            }
            let v: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| format!("IB-6: conflict body not JSON: {e}"))?;
            validate_conflict(&v)?;
            Ok(ErrorClass::Conflict)
        }
        500 => {
            expect_text(headers.content_type, status)?;
            let text = String::from_utf8_lossy(body);
            if !text.starts_with("Internal server error") {
                return Err("IB-7: 500 body lacks the mandated prefix".into());
            }
            Ok(ErrorClass::Internal)
        }
        other => Err(format!(
            "INV-27: status {other} is outside the closed taxonomy"
        )),
    }
}

/// Watermark pair from response headers (RP-9 / IB-4). IB-4 pins a decimal:
/// a numeral that does not round-trip (`+54`, `054`) is a wire deviation, not
/// a value to normalize away.
pub fn validate_watermark(number: Option<&str>, hash: Option<&str>) -> Result<BlockRef, String> {
    let raw = number.ok_or("INV-24: missing finalized-head number header")?;
    let number = raw
        .parse::<u64>()
        .map_err(|e| format!("INV-24: unparsable finalized-head number: {e}"))?;
    if number.to_string() != raw {
        return Err(format!(
            "IB-4: finalized-head number {raw:?} is not a canonical decimal"
        ));
    }
    let hash = hash.ok_or("INV-24: missing finalized-head hash header")?;
    Ok(BlockRef {
        number,
        hash: hash.to_string(),
    })
}

/// INV-24: within one read, the finalized watermark never exceeds the head.
/// `head` is whatever head figure the same read context exposes — the last
/// DATA record, or a paired `/head` read under quiescence.
pub fn validate_watermark_bounds(finalized: &BlockRef, head: &BlockRef) -> Result<(), String> {
    if finalized.number > head.number {
        return Err(format!(
            "INV-24: finalized watermark {} is above the head {}",
            finalized.number, head.number
        ));
    }
    if finalized.number == head.number && finalized.hash != head.hash {
        return Err(format!(
            "INV-24: finalized watermark and head disagree at height {}: {} vs {}",
            finalized.number, finalized.hash, head.hash
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mb(n: u64, h: &str, pn: u64, ph: &str) -> ModelBlock {
        ModelBlock {
            number: n,
            hash: h.into(),
            parent_number: pn,
            parent_hash: ph.into(),
        }
    }

    fn line(n: u64, h: &str, pn: u64, ph: &str) -> String {
        format!(
            "{{\"number\":{n},\"parentNumber\":{pn},\"hash\":\"{h}\",\"parentHash\":\"{ph}\"}}\n"
        )
    }

    /// RP-8: the eligible chain is the backfill prefix spliced onto the
    /// snapshot, so a response may — and must — start below the snapshot.
    #[test]
    fn backfill_prefix_is_eligible_and_mandatory() {
        let chain = [
            mb(8, "h8", 7, "h7"),
            mb(9, "h9", 8, "h8"),
            mb(10, "h10", 9, "h9"),
        ];
        let oracle = DataOracle {
            ledger: None,
            eligible: Some(&chain),
        };
        let full = line(8, "h8", 7, "h7") + &line(9, "h9", 8, "h8") + &line(10, "h10", 9, "h9");
        validate_data(&full, 8, sim_extract, &oracle).expect("backfill splice is conforming");
        let skipped = line(10, "h10", 9, "h9");
        assert!(validate_data(&skipped, 8, sim_extract, &oracle).is_err());
    }

    /// The first record has no predecessor to link against, so its parentRef
    /// is only checked against the eligible chain.
    #[test]
    fn forged_parent_on_the_first_record_is_caught() {
        let chain = [mb(9, "h9", 8, "h8"), mb(10, "h10", 9, "h9")];
        let oracle = DataOracle {
            ledger: None,
            eligible: Some(&chain),
        };
        let forged = line(10, "h10", 8, "h8");
        let err = validate_data(&forged, 10, sim_extract, &oracle).unwrap_err();
        assert!(err.starts_with("DEF-5"), "{err}");
    }

    #[test]
    fn watermark_numerals_must_round_trip() {
        // IB-4 pins a decimal; u64::from_str also accepts `+` and leading
        // zeros, which would normalize a wire-format deviation away.
        assert!(validate_watermark(Some("54"), Some("h")).is_ok());
        for bad in ["+54", "054", " 54", ""] {
            assert!(
                validate_watermark(Some(bad), Some("h")).is_err(),
                "non-canonical numeral {bad:?} accepted"
            );
        }
    }

    fn text_headers() -> WireHeaders<'static> {
        WireHeaders {
            content_type: Some("text/plain"),
            ..WireHeaders::default()
        }
    }

    #[test]
    fn readiness_negative_body_is_pinned() {
        assert_eq!(
            validate_outcome(503, &text_headers(), b"false").unwrap(),
            Outcome::Unavailable
        );
        assert!(validate_outcome(503, &text_headers(), b"true").is_err());
    }

    #[test]
    fn empty_form_requires_watermarks() {
        // IB-5: 204 must carry the finalized-head headers.
        let err = validate_error(204, &WireHeaders::default(), b"").unwrap_err();
        assert!(err.starts_with("IB-5"), "{err}");
        let with = WireHeaders {
            content_type: None,
            finalized_number: Some("54"),
            finalized_hash: Some("0xabc"),
            ..WireHeaders::default()
        };
        assert_eq!(validate_error(204, &with, b"").unwrap(), ErrorClass::Empty);
    }

    #[test]
    fn conflict_must_not_carry_watermarks() {
        // IB-6: no finalized-head headers on a conflict.
        let body = br#"{"previousBlocks":[{"number":3,"hash":"0xa"}]}"#;
        let with = WireHeaders {
            content_type: Some("application/json"),
            finalized_number: Some("54"),
            finalized_hash: None,
            ..WireHeaders::default()
        };
        let err = validate_error(409, &with, body).unwrap_err();
        assert!(err.starts_with("IB-6"), "{err}");
        let without = WireHeaders {
            content_type: Some("application/json"),
            ..WireHeaders::default()
        };
        assert_eq!(
            validate_error(409, &without, body).unwrap(),
            ErrorClass::Conflict
        );
    }

    fn zstd_frame(text: &str) -> Vec<u8> {
        zstd::encode_all(text.as_bytes(), 1).unwrap()
    }

    fn gzip_member(text: &str) -> Vec<u8> {
        use flate2::write::GzEncoder;
        use std::io::Write;
        let mut e = GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(text.as_bytes()).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn conflict_refs_are_bounded() {
        let refs = |n: usize| {
            serde_json::json!({
                "previousBlocks": (0..n)
                    .map(|i| serde_json::json!({"number": i, "hash": format!("h{i}")}))
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(
            validate_conflict(&refs(MAX_FORK_REFS)).unwrap().len(),
            MAX_FORK_REFS
        );
        // RP-7 bounds `prev`; an unbounded list is a response a client must
        // buffer without limit, so the ceiling is part of the contract.
        let err = validate_conflict(&refs(MAX_FORK_REFS + 1)).unwrap_err();
        assert!(err.contains("P-FORK-REFS-MAX"), "{err}");
    }

    #[test]
    fn framing_accepts_one_frame_per_record() {
        for (enc, frame) in [
            ("zstd", zstd_frame as fn(&str) -> Vec<u8>),
            ("gzip", gzip_member as fn(&str) -> Vec<u8>),
        ] {
            let mut body = frame("{\"n\":1}\n");
            body.extend(frame("{\"n\":2}\n"));
            body.extend(frame("{\"n\":3}\n"));
            let records = validate_framing(&body, enc).unwrap();
            assert_eq!(records.len(), 3, "{enc}: one frame per record");
            assert_eq!(records[1], "{\"n\":2}\n");
        }
    }

    #[test]
    fn framing_rejects_records_glued_into_one_frame() {
        // Whole-stream decoding cannot tell this from the case above: the
        // decoded bytes are identical. REQ-6 is about the framing, so the
        // validator must reject it (and a client truncating mid-frame would
        // lose a whole block, not a record).
        for (enc, frame) in [
            ("zstd", zstd_frame as fn(&str) -> Vec<u8>),
            ("gzip", gzip_member as fn(&str) -> Vec<u8>),
        ] {
            let glued = frame("{\"n\":1}\n{\"n\":2}\n");
            assert_eq!(
                decode_body(&glued, enc).unwrap(),
                "{\"n\":1}\n{\"n\":2}\n",
                "{enc}: the glued body decodes fine as a stream"
            );
            let err = validate_framing(&glued, enc).unwrap_err();
            assert!(err.contains("frames are per block"), "{enc}: {err}");
        }
    }

    #[test]
    fn framing_cuts_at_every_frame_boundary() {
        // ADR-10: truncation at a record boundary is a normal outcome, so
        // every prefix must decode to exactly the records before the cut.
        let mut body = zstd_frame("{\"n\":1}\n");
        let first = body.len();
        body.extend(zstd_frame("{\"n\":2}\n"));
        assert_eq!(validate_framing(&body[..first], "zstd").unwrap().len(), 1);
        assert_eq!(validate_framing(&body, "zstd").unwrap().len(), 2);
        // A cut inside a frame is not a record boundary and must not pass as
        // one: the validator reports the truncation instead of a short body.
        assert!(validate_framing(&body[..first + 4], "zstd").is_err());
    }

    #[test]
    fn body_content_type_is_mandatory() {
        assert!(validate_error(400, &WireHeaders::default(), b"bad request").is_err());

        let conflict = br#"{"previousBlocks":[{"number":3,"hash":"0xa"}]}"#;
        assert!(validate_error(409, &WireHeaders::default(), conflict).is_err());

        let fake_text = WireHeaders {
            content_type: Some("text/plain-evil"),
            ..WireHeaders::default()
        };
        assert!(validate_error(400, &fake_text, b"bad request").is_err());

        let fake_json = WireHeaders {
            content_type: Some("application/json-seq"),
            ..WireHeaders::default()
        };
        assert!(validate_error(409, &fake_json, conflict).is_err());
    }

    #[test]
    fn data_headers_bind_encoding_vary_and_watermark() {
        let headers = WireHeaders {
            content_type: Some("text/plain; charset=UTF-8"),
            content_encoding: Some("zstd"),
            vary: Some("Origin, Accept-Encoding"),
            finalized_number: Some("54"),
            finalized_hash: Some("0xabc"),
        };
        assert_eq!(
            validate_data_headers(&headers, "gzip, zstd").unwrap(),
            BlockRef {
                number: 54,
                hash: "0xabc".into()
            }
        );

        let missing_vary = WireHeaders {
            vary: None,
            ..headers
        };
        assert!(validate_data_headers(&missing_vary, "zstd").is_err());
    }

    #[test]
    fn watermark_bounds_hold_within_one_read() {
        let f = |n: u64, h: &str| BlockRef {
            number: n,
            hash: h.into(),
        };
        assert!(validate_watermark_bounds(&f(5, "a"), &f(6, "b")).is_ok());
        assert!(validate_watermark_bounds(&f(6, "a"), &f(6, "a")).is_ok());
        // Finalized above the head, or a different block at the head's height,
        // breaches INV-24.
        assert!(validate_watermark_bounds(&f(7, "a"), &f(6, "b")).is_err());
        assert!(validate_watermark_bounds(&f(6, "a"), &f(6, "b")).is_err());
    }
}
