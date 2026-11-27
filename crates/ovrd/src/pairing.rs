//! One-shot pairing codes. `ovr pair` prints one after the initiator's fingerprint, as `FINGERPRINT-CODE`, so the
//! host checks whom it dialed before it sends the code, and only the initiator ever sees it.

use std::time::{Duration, Instant};

use crate::tls;

/// How long a printed code stays valid.
pub const LIFETIME: Duration = Duration::from_mins(10);

/// Wrong attempts tolerated before every outstanding code is revoked.
const MAX_FAILURES: u32 = 5;

/// Crockford base32, which avoids characters that are easy to misread.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

const LEN: usize = 8;

/// Codes issued by `ovr pair` and not yet used.
#[derive(Default)]
pub struct Codes {
    issued: Vec<(String, Instant)>,
    failures: u32,
}

impl Codes {
    /// Issues a new code, formatted for display as `XXXX-XXXX`.
    ///
    /// # Errors
    ///
    /// Returns an error when the system random number generator fails.
    pub fn issue(&mut self) -> Result<String, ring::error::Unspecified> {
        let mut bytes = [0; LEN];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)?;
        let code: String = bytes
            .iter()
            .map(|byte| char::from(ALPHABET[usize::from(byte % 32)]))
            .collect();
        self.issued.push((code.clone(), Instant::now()));
        Ok(format!("{}-{}", &code[..LEN / 2], &code[LEN / 2..]))
    }

    /// Consumes `code`, in canonical form, returning whether it was outstanding. Repeated failures revoke every
    /// code.
    pub fn redeem(&mut self, code: &str) -> bool {
        self.issued
            .retain(|(_, issued)| issued.elapsed() < LIFETIME);
        if let Some(index) = self.issued.iter().position(|(issued, _)| issued == code) {
            self.failures = 0;
            self.issued.swap_remove(index);
            return true;
        }
        self.failures += 1;
        if self.failures >= MAX_FAILURES {
            self.issued.clear();
            self.failures = 0;
        }
        false
    }
}

/// Splits `FINGERPRINT-CODE`, as `ovr pair` prints it, into the initiator's fingerprint and the canonical code.
#[must_use]
pub fn parse(token: &str) -> Option<(String, String)> {
    let (fingerprint, code) = token.trim().split_once('-')?;
    Some((tls::parse_fingerprint(fingerprint)?, normalize(code)?))
}

/// Canonical form of a typed code: case, dashes, and look-alike letters do not matter.
fn normalize(input: &str) -> Option<String> {
    let code: String = input
        .chars()
        .filter(|c| *c != '-' && !c.is_whitespace())
        .map(|c| match c.to_ascii_uppercase() {
            'I' | 'L' => '1',
            'O' => '0',
            c => c,
        })
        .collect();
    (code.len() == LEN && code.bytes().all(|b| ALPHABET.contains(&b))).then_some(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_codes_are_forgiving_but_strict_about_content() {
        assert_eq!(normalize("7kqx-m4rt").as_deref(), Some("7KQXM4RT"));
        assert_eq!(normalize(" 7KQX M4RT ").as_deref(), Some("7KQXM4RT"));
        assert_eq!(normalize("OILx-m4rt").as_deref(), Some("011XM4RT"));
        assert_eq!(normalize("7KQX-M4R"), None);
        assert_eq!(normalize("7KQX-M4RU"), None);
    }

    #[test]
    fn codes_are_single_use_and_revoked_after_repeated_failures()
    -> Result<(), ring::error::Unspecified> {
        let mut codes = Codes::default();
        let first = codes.issue()?.replace('-', "");
        let second = codes.issue()?.replace('-', "");

        assert!(codes.redeem(&first));
        assert!(!codes.redeem(&first));
        for _ in 1..MAX_FAILURES {
            assert!(!codes.redeem("00000000"));
        }
        assert!(!codes.redeem(&second), "the fifth failure revoked it");
        Ok(())
    }
}
