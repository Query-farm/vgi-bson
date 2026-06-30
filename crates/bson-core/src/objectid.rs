//! ObjectId helpers: 12-byte ↔ 24-char-hex conversion and embedded-timestamp
//! extraction (the first 4 bytes are seconds since the Unix epoch).

use bson::oid::ObjectId;

use crate::validate::DecodeError;

/// `bson.objectid_hex(blob)` — 12 raw bytes → 24-char lowercase hex.
/// Errors if the input is not exactly 12 bytes.
pub fn objectid_hex(bytes: &[u8]) -> Result<String, DecodeError> {
    let arr: [u8; 12] = bytes.try_into().map_err(|_| DecodeError::InvalidType)?;
    Ok(ObjectId::from_bytes(arr).to_hex())
}

/// `bson.objectid_from_hex(hex)` — 24-char hex → 12 raw bytes.
pub fn objectid_from_hex(hex: &str) -> Result<Vec<u8>, DecodeError> {
    let oid = ObjectId::parse_str(hex.trim()).map_err(|_| DecodeError::InvalidType)?;
    Ok(oid.bytes().to_vec())
}

/// Parse an ObjectId from either a 24-char hex `&str` or a 12-byte slice.
pub fn parse_objectid_any(hex: Option<&str>, bytes: Option<&[u8]>) -> Option<ObjectId> {
    if let Some(h) = hex {
        return ObjectId::parse_str(h.trim()).ok();
    }
    if let Some(b) = bytes {
        let arr: [u8; 12] = b.try_into().ok()?;
        return Some(ObjectId::from_bytes(arr));
    }
    None
}

/// The embedded creation timestamp of an ObjectId, in epoch milliseconds.
pub fn objectid_epoch_millis(oid: &ObjectId) -> i64 {
    oid.timestamp().timestamp_millis()
}

/// Format epoch milliseconds as an RFC 3339 / ISO-8601 UTC string (cast-ready to
/// DuckDB `TIMESTAMPTZ`).
pub fn epoch_millis_to_rfc3339(millis: i64) -> String {
    let secs = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) * 1_000_000) as u32;
    format_rfc3339(secs, nanos)
}

/// Format epoch seconds as an RFC 3339 UTC string.
pub fn epoch_secs_to_rfc3339(secs: i64) -> String {
    format_rfc3339(secs, 0)
}

/// Minimal civil-time formatter (UTC, proleptic Gregorian) — avoids a chrono dep
/// in the core crate. Valid for the full i64 second range.
fn format_rfc3339(secs: i64, nanos: u32) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    if nanos == 0 {
        format!(
            "{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}Z",
            year = year
        )
    } else {
        let millis = nanos / 1_000_000;
        format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}.{millis:03}Z")
    }
}

/// Convert a count of days since 1970-01-01 into a civil `(year, month, day)`.
/// Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let hex = "54759eb3c090d83494e2d804";
        let bytes = objectid_from_hex(hex).unwrap();
        assert_eq!(bytes.len(), 12);
        assert_eq!(objectid_hex(&bytes).unwrap(), hex);
    }

    #[test]
    fn rejects_bad_lengths() {
        assert!(objectid_hex(&[0u8; 11]).is_err());
        assert!(objectid_from_hex("zz").is_err());
    }

    #[test]
    fn embedded_timestamp() {
        // ObjectId whose leading 4 bytes are 0x54759eb3 = 1416979123 (2014-11-26).
        let oid = ObjectId::parse_str("54759eb3c090d83494e2d804").unwrap();
        let millis = objectid_epoch_millis(&oid);
        assert_eq!(millis, 1_416_994_483_000);
        assert_eq!(epoch_millis_to_rfc3339(millis), "2014-11-26T09:34:43Z");
    }

    #[test]
    fn epoch_zero_formats() {
        assert_eq!(epoch_secs_to_rfc3339(0), "1970-01-01T00:00:00Z");
    }
}
