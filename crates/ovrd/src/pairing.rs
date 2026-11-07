//! One-shot pairing codes. Both sides prove knowledge of the code with a MAC over both TLS fingerprints, so a
//! man in the middle, who must present different certificates, cannot complete pairing without the code.

use std::time::{Duration, Instant};

use ring::hmac;

use crate::tls::hex;

/// How long a printed code stays valid.
pub const LIFETIME: Duration = Duration::from_mins(10);

/// Wrong attempts tolerated before every outstanding code is revoked.
const MAX_FAILURES: u32 = 5;

/// Crockford base32, which avoids characters that are easy to misread.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

const LEN: usize = 8;

/// Codes issued by `ovrd pair` and not yet used.
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

    /// Consumes the code that `verify` accepts, if any is outstanding. Repeated failures revoke every code.
    pub fn redeem(&mut self, verify: impl Fn(&str) -> bool) -> Option<String> {
        self.issued
            .retain(|(_, issued)| issued.elapsed() < LIFETIME);
        if let Some(index) = self.issued.iter().position(|(code, _)| verify(code)) {
            self.failures = 0;
            return Some(self.issued.swap_remove(index).0);
        }
        self.failures += 1;
        if self.failures >= MAX_FAILURES {
            self.issued.clear();
            self.failures = 0;
        }
        None
    }
}

/// Canonical form of a typed code: case, dashes, and look-alike letters do not matter.
#[must_use]
pub fn normalize(input: &str) -> Option<String> {
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

/// What one side signs with the code to prove it took part in this exchange.
pub struct Proof(Vec<u8>);

impl Proof {
    /// The host's proof: it knows the code and saw these fingerprints.
    #[must_use]
    pub fn host(laptop: &str, host: &str, hostname: &str) -> Self {
        Self::of(&["ovr pair v1", laptop, host, hostname])
    }

    /// The laptop's proof in reply.
    #[must_use]
    pub fn laptop(laptop: &str, host: &str) -> Self {
        Self::of(&["ovr pair ok v1", laptop, host])
    }

    /// Length-prefixed parts, so that no two different part lists produce the same message.
    fn of(parts: &[&str]) -> Self {
        let mut message = Vec::new();
        for part in parts {
            message.extend_from_slice(&(part.len() as u64).to_le_bytes());
            message.extend_from_slice(part.as_bytes());
        }
        Self(message)
    }

    /// The hex MAC to send.
    #[must_use]
    pub fn sign(&self, code: &str) -> String {
        hex(hmac::sign(&key(code), &self.0).as_ref())
    }

    /// Whether `mac` is this proof signed with `code`, compared in constant time.
    #[must_use]
    pub fn verify(&self, code: &str, mac: &str) -> bool {
        unhex(mac).is_some_and(|tag| hmac::verify(&key(code), &self.0, &tag).is_ok())
    }
}

fn key(code: &str) -> hmac::Key {
    hmac::Key::new(hmac::HMAC_SHA256, code.as_bytes())
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
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
        let first = normalize(&codes.issue()?);
        let second = normalize(&codes.issue()?);

        assert_eq!(
            codes.redeem(|candidate| Some(candidate) == first.as_deref()),
            first
        );
        assert_eq!(
            codes.redeem(|candidate| Some(candidate) == first.as_deref()),
            None
        );
        for _ in 1..MAX_FAILURES {
            assert_eq!(codes.redeem(|_| false), None);
        }
        assert_eq!(
            codes.redeem(|candidate| Some(candidate) == second.as_deref()),
            None,
            "the fifth failure revoked it"
        );
        Ok(())
    }

    #[test]
    fn proofs_bind_the_code_and_both_identities() {
        let mac = Proof::host("laptop", "host", "winbox").sign("7KQXM4RT");
        assert!(Proof::host("laptop", "host", "winbox").verify("7KQXM4RT", &mac));
        assert!(!Proof::host("laptop", "host", "winbox").verify("7KQXM4RU", &mac));
        assert!(!Proof::host("mitm", "host", "winbox").verify("7KQXM4RT", &mac));
        assert!(!Proof::laptop("laptop", "host").verify("7KQXM4RT", &mac));
        assert!(!Proof::host("laptop", "host", "winbox").verify("7KQXM4RT", "zz"));
    }
}
