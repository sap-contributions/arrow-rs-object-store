// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::hdlfs::client::SAPHdlfsClient;
use crate::hdlfs::client::SAPHdlfsConfig;
use crate::hdlfs::credential::SAPHdlfsCredential;

use crate::hdlfs::SAPHdlfs;
use crate::{ClientOptions, Result, RetryConfig};
use reqwest::{ClientBuilder, Identity};
use std::fmt::{Display, Formatter};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::client::HttpClient;
use ring::signature::RsaKeyPair;
use rustls_pemfile::Item;
use serde::{Deserialize, Serialize};
use std::io::BufReader;
use std::io::Cursor;
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Failed to parse URL {0}: {1}")]
    UrlParseError(String, url::ParseError),
    #[error("Config error: {0}")]
    ConfigError(#[from] crate::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("Invalid key: {0}")]
    InvalidKey(String),
    #[error("Invalid certificate: {0}")]
    InvalidCertificate(String),
    #[error("Configuration key: '{}' is not known.", key)]
    UnknownConfigurationKey { key: String },
    #[error(
        "Unknown url scheme cannot be parsed into storage location: {}",
        scheme
    )]
    UnknownUrlScheme { scheme: String },
    #[error("Unable parse source url. Url: {}, Error: {}", url, source)]
    UnableToParseUrl {
        source: url::ParseError,
        url: String,
    },
    #[error("URL did not match any known pattern for scheme: {}", url)]
    UrlNotRecognised { url: String },
}

impl From<Error> for crate::Error {
    fn from(source: Error) -> Self {
        match source {
            _ => Self::Generic {
                store: crate::hdlfs::STORE,
                source: Box::new(source),
            },
        }
    }
}

/// Builder for SAP HANA Cloud, Data Lake Files (hdlfs) client
#[derive(Default, Clone)]
pub struct SAPHdlfsBuilder {
    container_id: String,
    credential: SAPHdlfsCredential,
    url: Option<String>,
    trace: crate::config::ConfigValue<bool>,
    endpoint: String,
    use_emulator: crate::config::ConfigValue<bool>,
    retry_config: RetryConfig,
    client_options: ClientOptions,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Deserialize, Serialize)]
#[non_exhaustive]
/// Configuration keys for SAP HANA Cloud, Data Lake Files (hdlfs) client
pub enum SAPHdlfsConfigKey {
    /// The name of the hdlfs storage private key
    ///
    /// Supported keys:
    /// - `hdlfs_storage_private_key`
    /// - `private_key`
    PrivateKey,

    /// The name of the hdlfs storage certificate
    ///
    /// Supported keys:
    /// - `hdlfs_storage_certificate`
    /// - `certificate`
    Certificate,

    /// The name of the hdlfs storage certificate
    ///
    /// Supported keys:
    /// - `hdlfs_storage_endpoint`
    /// - `endpoint`
    Endpoint,

    /// Container id
    ///
    /// Supported keys:
    /// - `hdlfs_container_id`
    /// - `container_id`
    ContainerId,

    /// Use object store with hdlfs storage emulator
    ///
    /// Supported keys:
    /// - `hdlfs_storage_use_emulator`
    /// - `use_emulator`
    UseEmulator,

    /// Use object store with hdlfs storage emulator
    ///
    /// Supported keys:
    /// - `hdlfs_storage_trace`
    /// - `trace`
    Trace,

    /// Client options
    Client(crate::client::ClientConfigKey),
}

impl AsRef<str> for crate::hdlfs::builder::SAPHdlfsConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::PrivateKey => "hdlfs_storage_private_key",
            Self::Certificate => "hdlfs_storage_certificate",
            Self::Endpoint => "hdlfs_storage_endpoint",
            Self::ContainerId => "hdlfs_container_id",
            Self::UseEmulator => "hdlfs_storage_use_emulator",
            Self::Trace => "hdlfs_storage_trace",
            Self::Client(key) => key.as_ref(),
        }
    }
}

impl FromStr for SAPHdlfsConfigKey {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "hdlfs_storage_private_key" | "private_key" => Ok(Self::PrivateKey),
            "hdlfs_storage_certificate" | "certificate" => Ok(Self::Certificate),
            "hdlfs_storage_endpoint" | "endpoint" => Ok(Self::Endpoint),
            "hdlfs_container_id" | "container_id" => Ok(Self::ContainerId),
            "hdlfs_storage_use_emulator" | "use_emulator" => Ok(Self::UseEmulator),
            "hdlfs_storage_trace" | "trace" => Ok(Self::Trace),
            // Delegate to ClientConfigKey for client options
            other => {
                if let Ok(client_key) = crate::client::ClientConfigKey::from_str(other) {
                    Ok(Self::Client(client_key))
                } else {
                    Err(Error::UnknownConfigurationKey { key: s.to_string() }.into())
                }
            }
        }
    }
}

impl std::fmt::Debug for crate::hdlfs::builder::SAPHdlfsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SAPHdlfsBuilder {{ container_id: {:?} }}",
            self.container_id
        )
    }
}

impl SAPHdlfsBuilder {
    /// Create a new [`crate::hdlfs::builder::SAPHdlfsBuilder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a new [`crate::hdlfs::builder::SAPHdlfsBuilder`] from environment variables.
    pub fn from_env() -> Self {
        let mut builder = Self::default();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("HDLFS_") {
                    if let Ok(config_key) = key.to_ascii_lowercase().parse() {
                        builder = builder.with_config(config_key, value);
                    }
                }
            }
        }

        builder
    }

    /// Set the container ID
    pub fn with_container_id(mut self, container_id: impl Into<String>) -> Self {
        self.container_id = container_id.into();
        self
    }

    /// Set the credential
    pub fn with_credential(mut self, credential: SAPHdlfsCredential) -> Self {
        self.credential = credential;
        self
    }

    /// Set the endpoint
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Set the URL for the SAP HANA Cloud HDLFS service.
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set a configuration value by key.
    pub fn with_config(mut self, key: SAPHdlfsConfigKey, value: impl Into<String>) -> Self {
        let value = value.into();
        match key {
            SAPHdlfsConfigKey::PrivateKey => self.credential.key_path = value.into(),
            SAPHdlfsConfigKey::Certificate => self.credential.cert_path = value.into(),
            SAPHdlfsConfigKey::Endpoint => self.endpoint = value,
            SAPHdlfsConfigKey::ContainerId => self.container_id = value,
            SAPHdlfsConfigKey::UseEmulator => {
                if let Ok(val) = value.parse::<bool>() {
                    self.use_emulator = val.into();
                }
            }
            SAPHdlfsConfigKey::Trace => {
                if let Ok(val) = value.parse::<bool>() {
                    self.trace = val.into();
                }
            }
            SAPHdlfsConfigKey::Client(key) => {
                self.client_options = self.client_options.with_config(key, value)
            }
        }
        self
    }

    /// Sets whether to use the HDLFS emulator instead of the real service.
    ///
    /// # Arguments
    ///
    /// * `use_emulator` - If true, the builder will configure the client to use the emulator.
    ///
    /// Returns the builder for method chaining.
    pub fn with_use_emulator(mut self, use_emulator: bool) -> Self {
        self.use_emulator = use_emulator.into();
        self
    }

    /// Sets whether to enable tracing for the client.
    pub fn with_trace(mut self, trace: bool) -> Self {
        self.trace = trace.into();
        self
    }
    
    /// Set the retry configuration
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Returns the configuration value associated with the given `HdlfsConfigKey`.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the configuration key to look up.
    ///
    /// # Returns
    ///
    /// An `Option<String>` containing the value if it exists, or `None` if the value is not set
    /// or the key is not recognized.
    pub fn get_config_value(
        &self,
        key: &crate::hdlfs::builder::SAPHdlfsConfigKey,
    ) -> Option<String> {
        match key {
            SAPHdlfsConfigKey::PrivateKey => {
                Some(self.credential.key_path.to_string_lossy().into_owned())
            }
            SAPHdlfsConfigKey::Certificate => {
                Some(self.credential.cert_path.to_string_lossy().into_owned())
            }
            SAPHdlfsConfigKey::Endpoint => Some(self.endpoint.clone()),
            SAPHdlfsConfigKey::ContainerId => Some(self.container_id.clone()),
            SAPHdlfsConfigKey::UseEmulator => Some(self.use_emulator.get().ok()?.to_string()),
            SAPHdlfsConfigKey::Trace => Some(self.trace.get().ok()?.to_string()),
            SAPHdlfsConfigKey::Client(key) => self.client_options.get_config_value(key),
        }
    }

    /// Sets properties on this builder based on a URL
    ///
    /// This is a separate member function to allow fallible computation to
    /// be deferred until [`Self::build`] which in turn allows deriving [`Clone`]
    fn parse_url(&mut self, url: &str) -> Result<()> {
        let parsed = Url::parse(url).map_err(|source| Error::UnableToParseUrl {
            source,
            url: url.to_string(),
        })?;

        let host = parsed.host_str().ok_or_else(|| Error::UrlNotRecognised {
            url: url.to_string(),
        })?;

        match parsed.scheme() {
            "hdlfs" | "https" => {
                // Example: 7e698a97-a320-464d-9950-06ceee326fd2.files.hdl.canary-eu10.hanacloud.ondemand.com
                eprintln!("host: {}", host);
                let parts: Vec<&str> = host.split('.').collect();
                if parts.len() != 7 {
                    return Err(
                        Error::UrlParseError(url.to_string(), url::ParseError::EmptyHost).into(),
                    );
                }
                self.container_id = parts[0].to_string();
                self.endpoint = host.to_string()
            }

            scheme => {
                let scheme = scheme.to_string();
                return Err(Error::UnknownUrlScheme { scheme }.into());
            }
        }
        Ok(())
    }

    /// Sets properties on this builder based on a URL
    ///
    /// This is a separate member function to allow fallible computation to
    /// be deferred until [`Self::build`] which in turn allows deriving [`Clone`]
    fn parse_endpoint(&mut self, custom_endpoint: &str) -> Result<()> {
        eprintln!("custom_endpoint: [{}]", custom_endpoint);
        let parts: Vec<&str> = custom_endpoint.split('.').collect();
        if parts.len() != 7 {
            return Err(Error::UrlParseError(
                custom_endpoint.to_string(),
                url::ParseError::EmptyHost,
            )
            .into());
        }
        self.container_id = parts[0].to_string();
        eprintln!("container_id: [{}]", self.container_id);
        Ok(())
    }

    /// Build the SAP HANA Cloud HDLFS client
    pub fn build(mut self) -> Result<SAPHdlfs> {
        let options = ClientOptions::new().with_timeout(Duration::from_secs(60));
        let use_emulator = self.use_emulator.get()?;
        let trace = self.trace.get()?;

        if let Some(url) = self.url.take() {
            eprintln!("parse_url: {}", url);
            self.parse_url(&url)?;
        } else {
            self.parse_endpoint(&self.endpoint.clone())?;
        }

        // Read and parse private key (support PKCS#1 and PKCS#8)
        let cert_pem = fs::read(&self.credential.cert_path)
            .map_err(|e| Error::InvalidCertificate(format!("Failed to read cert: {e}")))?;
        let key_pem = fs::read(&self.credential.key_path)
            .map_err(|e| Error::InvalidKey(format!("Failed to read key: {e}")))?;
        let mut cursor = Cursor::new(&key_pem);
        let mut reader = BufReader::new(&mut cursor);

        let key_der = match rustls_pemfile::read_one(&mut reader)
            .map_err(|e| Error::InvalidKey(format!("Failed to parse PEM: {e}")))?
        {
            Some(Item::Pkcs8Key(key)) => key.secret_pkcs8_der().to_vec(),
            Some(Item::Pkcs1Key(key)) => key.secret_pkcs1_der().to_vec(),
            _ => return Err(Error::InvalidKey("Unsupported key type".to_string()).into()),
        };

        RsaKeyPair::from_pkcs8(&key_der).map_err(|_| {
            Error::InvalidKey("Only PKCS#8 or PKCS#1 private keys are supported".to_string())
        })?;

        // Combine cert and key for reqwest Identity
        let mut identity_pem = cert_pem.clone();
        identity_pem.extend_from_slice(&key_pem);
        let identity = Identity::from_pem(&identity_pem).map_err(Error::from)?;

        let reqwest_client = ClientBuilder::new()
            .use_rustls_tls()
            .redirect(reqwest::redirect::Policy::limited(5))
            .identity(identity)
            .build()
            .map_err(Error::from)?;

        let service_url = format!("https://{}/webhdfs/v1/", self.endpoint);
        eprintln!("service_url: [{}]", service_url);
        let parsed_url = Url::parse(&service_url).map_err(Error::from)?;

        let retry_config = RetryConfig {
            max_retries: 5,
            ..Default::default()
        };

        let config = SAPHdlfsConfig::new(
            self.container_id,
            parsed_url,
            use_emulator,
            trace,
            retry_config,
            options,
        );

        let client = SAPHdlfsClient::with_config(HttpClient::new(reqwest_client), config);
        Ok(SAPHdlfs {
            client: Arc::new(client),
        })
    }
}

impl Display for SAPHdlfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HdlfsBuilder(endpoint={})", self.endpoint)
    }
}
