//! Trusting workloads that an identity provider vouches for, so that hosts such as CI jobs can dial in without a
//! pairing code. Each provider issues the host a token for the audience `ovr:<initiator fingerprint>`; the
//! initiator checks it against the provider's published keys and its trust rules, and names the host after the
//! workload. Nothing is recorded on either side beyond the rules.
//!
//! GitHub Actions is the only provider so far. Another is a variant of [`Provider`] and [`Rule`] and a module like
//! [`github`].

pub mod github;
mod jwt;

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::state;

/// Bounds each request to a provider.
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    Github,
}

impl Provider {
    pub const ALL: [Self; 1] = [Self::Github];

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Github => "github",
        }
    }

    /// Whether this process can get tokens from the provider.
    ///
    /// # Errors
    ///
    /// Returns an error explaining what is missing.
    pub fn check_environment(self) -> Result<(), String> {
        match self {
            Self::Github => github::check_environment(),
        }
    }

    /// Gets a fresh token for the initiator with `fingerprint`.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider does not issue one.
    pub async fn token(self, fingerprint: &str) -> Result<String, String> {
        match self {
            Self::Github => github::token(&audience(fingerprint)).await,
        }
    }

    /// Where the workload's files are, such as a GitHub Actions job's `GITHUB_WORKSPACE`: where runs without a
    /// checkout start, instead of the home directory.
    #[must_use]
    pub fn workspace(self) -> Option<PathBuf> {
        match self {
            Self::Github => github::workspace(),
        }
    }

    /// What tells apart hosts of one workload.
    #[must_use]
    pub fn label(self) -> String {
        match self {
            Self::Github => github::label(),
        }
    }
}

impl FromStr for Provider {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|provider| provider.name() == name)
            .ok_or_else(|| format!("unknown identity provider {name:?}; use github"))
    }
}

impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The audience tokens for the initiator with `fingerprint` are issued for, so that no other service accepts them
/// and they are accepted by no other initiator.
#[must_use]
pub fn audience(fingerprint: &str) -> String {
    format!("ovr:{fingerprint}")
}

/// What a host is given instead of a pairing code: `PROVIDER:FINGERPRINT`. The fingerprint pins the initiator, as
/// pairing would have, before any token is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Federation {
    pub provider: Provider,
    pub initiator: String,
}

impl FromStr for Federation {
    type Err = String;

    fn from_str(spec: &str) -> Result<Self, String> {
        let (provider, initiator) = spec
            .split_once(':')
            .ok_or_else(|| format!("{spec:?} is not PROVIDER:FINGERPRINT"))?;
        let initiator = initiator.to_ascii_lowercase();
        if initiator.len() != 64 || !initiator.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!(
                "{initiator:?} is not an initiator fingerprint; `ovrd trust` prints it"
            ));
        }
        Ok(Self {
            provider: provider.parse()?,
            initiator,
        })
    }
}

impl fmt::Display for Federation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.provider, self.initiator)
    }
}

/// A trust rule, as in `github:OWNER/REPO`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum Rule {
    Github(github::Rule),
}

impl Rule {
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Github(a), Self::Github(b)) => a.same(b),
        }
    }
}

impl FromStr for Rule {
    type Err = String;

    fn from_str(spec: &str) -> Result<Self, String> {
        let (provider, rest) = spec.split_once(':').ok_or_else(|| {
            format!("{spec:?} is not a trust rule; it looks like github:OWNER/REPO")
        })?;
        match provider.parse()? {
            Provider::Github => github::Rule::parse(rest).map(Self::Github),
        }
    }
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Github(rule) => write!(f, "github:{}", rule.spec()),
        }
    }
}

/// The initiator's side: the rules in the data directory, and a verifier per provider.
pub struct Trust {
    dir: PathBuf,
    /// Serialises changes to the rules, which verification makes too when it records IDs.
    rules: Mutex<()>,
    github: github::Verifier,
}

impl Trust {
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            rules: Mutex::default(),
            github: github::Verifier::default(),
        }
    }

    /// The rules, in the order they were added.
    ///
    /// # Errors
    ///
    /// Returns an error when the rules cannot be read.
    pub fn rules(&self) -> std::io::Result<Vec<Rule>> {
        state::trust_rules(&self.dir)
    }

    /// Adds `rule` unless it is there already.
    ///
    /// # Errors
    ///
    /// Returns an error when the rules cannot be read or written.
    pub fn add(&self, rule: Rule) -> std::io::Result<()> {
        self.update(|rules| {
            if rules.iter().any(|known| known.same(&rule)) {
                return false;
            }
            rules.push(rule);
            true
        })
    }

    /// Removes `rule`, returning whether it was there.
    ///
    /// # Errors
    ///
    /// Returns an error when the rules cannot be read or written.
    pub fn remove(&self, rule: &Rule) -> std::io::Result<bool> {
        let mut removed = false;
        self.update(|rules| {
            let before = rules.len();
            rules.retain(|known| !known.same(rule));
            removed = rules.len() != before;
            removed
        })?;
        Ok(removed)
    }

    fn update(&self, change: impl FnOnce(&mut Vec<Rule>) -> bool) -> std::io::Result<()> {
        let _guard = self.rules.lock().unwrap_or_else(PoisonError::into_inner);
        let mut rules = state::trust_rules(&self.dir)?;
        if change(&mut rules) {
            state::set_trust_rules(&self.dir, &rules)?;
        }
        Ok(())
    }

    /// Checks `token` from `provider` for the initiator with `fingerprint` against the rules, and returns the
    /// workload's name, which names the host, and what tells which run of it the host serves.
    ///
    /// # Errors
    ///
    /// Returns the reason the host is refused.
    pub async fn admit(
        &self,
        provider: &str,
        token: &str,
        fingerprint: &str,
    ) -> Result<(String, String), String> {
        match provider.parse()? {
            Provider::Github => {
                let claims = self.github.verify(token, &audience(fingerprint)).await?;
                let mut admission = github::Admission::No;
                self.update(|rules| {
                    for Rule::Github(rule) in rules.iter_mut() {
                        admission = rule.admit(&claims);
                        if admission != github::Admission::No {
                            break;
                        }
                    }
                    admission == github::Admission::Pinned
                })
                .map_err(|err| format!("cannot read the trust rules: {err}"))?;
                match admission {
                    github::Admission::Yes | github::Admission::Pinned => {
                        Ok((claims.workload(), claims.about()))
                    }
                    github::Admission::Replaced => Err(format!(
                        "{} is not the account or repository that was trusted, which has been renamed or deleted",
                        claims.repository
                    )),
                    github::Admission::No => Err(format!(
                        "this initiator does not trust {}; run `ovrd trust github:{}` there",
                        claims.repository, claims.repository
                    )),
                }
            }
        }
    }
}

/// Seconds since the Unix epoch.
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// GETs `url` over HTTPS, with `bearer` as the authorization if given, and returns the body.
async fn http_get(url: String, bearer: Option<String>) -> Result<Vec<u8>, String> {
    tokio::task::spawn_blocking(move || {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(HTTP_TIMEOUT))
            .build()
            .new_agent();
        let mut request = agent.get(&url);
        if let Some(bearer) = bearer {
            request = request.header("Authorization", format!("Bearer {bearer}"));
        }
        let host = url.split('/').nth(2).unwrap_or_default().to_owned();
        request
            .call()
            .and_then(|mut response| response.body_mut().read_to_vec())
            .map_err(|err| format!("cannot reach {host}: {err}"))
    })
    .await
    .map_err(|err| err.to_string())?
}
