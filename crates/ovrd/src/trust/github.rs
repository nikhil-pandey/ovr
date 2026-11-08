//! GitHub Actions: jobs prove which repository they run for with an OIDC token from the runner.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::{http_get, jwt};

const ISSUER: &str = "https://token.actions.githubusercontent.com";
const JWKS: &str = "https://token.actions.githubusercontent.com/.well-known/jwks";

/// The variables the runner sets when the job may request tokens, that is, has `permissions: id-token: write`.
const REQUEST_URL: &str = "ACTIONS_ID_TOKEN_REQUEST_URL";
const REQUEST_TOKEN: &str = "ACTIONS_ID_TOKEN_REQUEST_TOKEN";

/// Keys are fetched again for an unknown key ID, but no more often than this.
const REFRESH: Duration = Duration::from_mins(1);

/// Trusts every repository of `owner`, or only `owner/repo`. Names are matched without regard to case, as GitHub
/// does; the IDs are recorded from the first token that matches, so that an account or repository created later
/// under a released name does not inherit the trust.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_id: Option<String>,
}

impl Rule {
    /// Parses `OWNER` or `OWNER/REPO`.
    ///
    /// # Errors
    ///
    /// Returns an error when either name is not a valid GitHub name.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let (owner, repo) = match spec.split_once('/') {
            Some((owner, repo)) => (owner, Some(repo)),
            None => (spec, None),
        };
        let valid = |name: &str| {
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        };
        if !valid(owner) || !repo.is_none_or(valid) {
            return Err(format!(
                "{spec:?} is not a GitHub owner or repository; use OWNER or OWNER/REPO"
            ));
        }
        Ok(Self {
            owner: owner.to_owned(),
            repo: repo.map(str::to_owned),
            owner_id: None,
            repo_id: None,
        })
    }

    /// `OWNER` or `OWNER/REPO`.
    #[must_use]
    pub fn spec(&self) -> String {
        match &self.repo {
            Some(repo) => format!("{}/{repo}", self.owner),
            None => self.owner.clone(),
        }
    }

    /// Whether the rule names the same owner and repository as `other`.
    #[must_use]
    pub fn same(&self, other: &Self) -> bool {
        self.spec().eq_ignore_ascii_case(&other.spec())
    }

    /// Whether `claims` satisfy this rule. Records the IDs the first time they do.
    pub fn admit(&mut self, claims: &Claims) -> Admission {
        if !self.owner.eq_ignore_ascii_case(&claims.repository_owner) {
            return Admission::No;
        }
        if let Some(repo) = &self.repo
            && !format!("{}/{repo}", self.owner).eq_ignore_ascii_case(&claims.repository)
        {
            return Admission::No;
        }
        if self
            .owner_id
            .as_ref()
            .is_some_and(|id| *id != claims.repository_owner_id)
            || (self.repo.is_some()
                && self
                    .repo_id
                    .as_ref()
                    .is_some_and(|id| *id != claims.repository_id))
        {
            return Admission::Replaced;
        }
        let pinned = self.owner_id.is_none() || (self.repo.is_some() && self.repo_id.is_none());
        self.owner_id = Some(claims.repository_owner_id.clone());
        if self.repo.is_some() {
            self.repo_id = Some(claims.repository_id.clone());
        }
        if pinned {
            Admission::Pinned
        } else {
            Admission::Yes
        }
    }
}

/// What a rule makes of a token's claims.
#[derive(Debug, PartialEq, Eq)]
pub enum Admission {
    /// It names another owner or repository.
    No,
    /// It names this one, but under the IDs of an account or repository that has since gone.
    Replaced,
    /// It admits the token.
    Yes,
    /// It admits the token and recorded the IDs, so the rule must be saved.
    Pinned,
}

/// The claims of a GitHub Actions token that trust rules look at. IDs are strings in GitHub's tokens.
#[derive(Debug, Deserialize)]
pub struct Claims {
    pub repository: String,
    pub repository_id: String,
    pub repository_owner: String,
    pub repository_owner_id: String,
}

/// Checks tokens against GitHub's signing keys, fetched when a token names one not seen yet.
#[derive(Default)]
pub struct Verifier {
    keys: Mutex<Keys>,
}

#[derive(Default)]
struct Keys {
    jwks: jwt::Jwks,
    fetched: Option<Instant>,
}

impl Keys {
    fn get(&self, kid: Option<&str>) -> Option<&jwt::Jwk> {
        self.jwks.keys.iter().find(|key| key.kid.as_deref() == kid)
    }
}

impl Verifier {
    /// Verifies `token` for `audience` and returns its claims.
    ///
    /// # Errors
    ///
    /// Returns an error when the token is not a valid GitHub Actions token for `audience`.
    pub async fn verify(&self, token: &str, audience: &str) -> Result<Claims, String> {
        const UNKNOWN: &str = "the token is signed with a key GitHub does not publish";
        let unverified = jwt::decode(token)?;
        let kid = unverified.header.kid.as_deref();
        {
            // Not held across the fetch, so that tokens with known keys never wait for GitHub.
            let mut keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(key) = keys.get(kid) {
                return unverified.verify(key, ISSUER, audience, super::now());
            }
            if keys.fetched.is_some_and(|at| at.elapsed() < REFRESH) {
                return Err(UNKNOWN.to_owned());
            }
            keys.fetched = Some(Instant::now());
        }
        let body = http_get(JWKS.to_owned(), None).await?;
        let jwks: jwt::Jwks = serde_json::from_slice(&body)
            .map_err(|err| format!("GitHub's signing keys are malformed: {err}"))?;
        let mut keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
        keys.jwks = jwks;
        let key = keys.get(kid).ok_or(UNKNOWN)?;
        unverified.verify(key, ISSUER, audience, super::now())
    }
}

/// Whether this process can request tokens: it runs in a GitHub Actions job allowed to.
///
/// # Errors
///
/// Returns an error explaining what is missing.
pub fn check_environment() -> Result<(), String> {
    if std::env::var_os(REQUEST_URL).is_some() && std::env::var_os(REQUEST_TOKEN).is_some() {
        return Ok(());
    }
    if std::env::var_os("GITHUB_ACTIONS").is_some() {
        Err(
            "this job cannot request an OIDC token; give it `permissions: id-token: write`"
                .to_owned(),
        )
    } else {
        Err("`github:` connects only from a GitHub Actions job".to_owned())
    }
}

/// Requests a token for `audience` from the runner.
///
/// # Errors
///
/// Returns an error when the runner does not issue one.
pub async fn token(audience: &str) -> Result<String, String> {
    #[derive(Deserialize)]
    struct Issued {
        value: String,
    }

    check_environment()?;
    let url = std::env::var(REQUEST_URL).map_err(|err| err.to_string())?;
    let bearer = std::env::var(REQUEST_TOKEN).map_err(|err| err.to_string())?;
    let separator = if url.contains('?') { '&' } else { '?' };
    let audience: String = audience
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect();
    let body = http_get(format!("{url}{separator}audience={audience}"), Some(bearer)).await?;
    let issued: Issued = serde_json::from_slice(&body)
        .map_err(|err| format!("the runner's token response is malformed: {err}"))?;
    Ok(issued.value)
}

/// What tells apart this job's host from the other jobs of its run: the job's ID in the workflow.
#[must_use]
pub fn label() -> String {
    std::env::var("GITHUB_JOB").unwrap_or_default()
}
