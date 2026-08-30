//! Talking to the daemon's HTTP surface, and printing what it answers.

use std::{error::Error, fs, path::Path};

use fleetd_fleet::base_url;

use serde::Serialize;
use serde_json::Value;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{validate_loaded_token, validate_secret_file};

pub(super) struct ApiClient {
    pub(super) server: String,
    pub(super) token: String,
    client: reqwest::Client,
}

impl ApiClient {
    pub(super) fn load(server: &str, token_file: Option<&Path>) -> MainResult<Self> {
        Ok(Self {
            server: base_url(server).to_owned(),
            token: load_client_token(token_file)?,
            client: reqwest::Client::new(),
        })
    }

    pub(super) fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .get(format!("{}{path}", self.server))
            .bearer_auth(&self.token)
    }

    pub(super) fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.server))
            .bearer_auth(&self.token)
    }

    pub(super) fn put(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .put(format!("{}{path}", self.server))
            .bearer_auth(&self.token)
    }
}

pub(super) async fn print_response(response: reqwest::Response) -> MainResult<()> {
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("fleetd returned {status}: {body}").into());
    }
    if !body.is_empty() {
        let value: Value = serde_json::from_str(&body)?;
        print_json(&value)?;
    }
    Ok(())
}

pub(super) fn load_client_token(token_file: Option<&Path>) -> MainResult<String> {
    match std::env::var("FLEETD_TOKEN") {
        Ok(token) => validate_loaded_token(&token),
        Err(std::env::VarError::NotPresent) => {
            let path = token_file.unwrap_or_else(|| Path::new(".fleetd/operator.token"));
            validate_secret_file(path)?;
            validate_loaded_token(&fs::read_to_string(path)?)
        }
        Err(std::env::VarError::NotUnicode(_)) => Err("FLEETD_TOKEN is not valid Unicode".into()),
    }
}

pub(super) fn print_json(value: &impl Serialize) -> MainResult<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
