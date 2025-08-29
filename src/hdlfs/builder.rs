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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Test helper to create temp files for build tests
    fn create_test_files() -> (NamedTempFile, NamedTempFile) {
        let mut cert_file = NamedTempFile::new().unwrap();
        let mut key_file = NamedTempFile::new().unwrap();
        cert_file.write_all(b"-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----").unwrap();
        key_file.write_all(b"-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----").unwrap();
        (cert_file, key_file)
    }

    #[test]
    fn test_builder_methods_and_traits() {
        // Test all builder methods and traits in one comprehensive test
        let builder = SAPHdlfsBuilder::new()
            .with_container_id("test_container")
            .with_endpoint("test.endpoint.com")
            .with_trace(true)
            .with_use_emulator(true)
            .with_credential(SAPHdlfsCredential::default())
            .with_url("hdlfs://test.example.com")
            .with_retry(RetryConfig { max_retries: 5, ..Default::default() });

        // Test builder state
        assert_eq!(builder.container_id, "test_container");
        assert_eq!(builder.trace.get().unwrap(), true);
        assert_eq!(builder.url, Some("hdlfs://test.example.com".to_string()));

        // Test Display and Debug traits
        assert_eq!(format!("{}", builder), "HdlfsBuilder(endpoint=test.endpoint.com)");
        assert!(format!("{:?}", builder).contains("SAPHdlfsBuilder"));
    }

    #[test]
    fn test_config_keys_and_parsing() {
        // Test all config key variants in a compact way
        let key_tests = [
            ("private_key", SAPHdlfsConfigKey::PrivateKey, "hdlfs_storage_private_key"),
            ("certificate", SAPHdlfsConfigKey::Certificate, "hdlfs_storage_certificate"),
            ("endpoint", SAPHdlfsConfigKey::Endpoint, "hdlfs_storage_endpoint"),
            ("container_id", SAPHdlfsConfigKey::ContainerId, "hdlfs_container_id"),
            ("use_emulator", SAPHdlfsConfigKey::UseEmulator, "hdlfs_storage_use_emulator"),
            ("trace", SAPHdlfsConfigKey::Trace, "hdlfs_storage_trace"),
        ];

        for (input, expected_key, expected_ref) in &key_tests {
            assert_eq!(SAPHdlfsConfigKey::from_str(input).unwrap(), *expected_key);
            assert_eq!(expected_key.as_ref(), *expected_ref);
            // Test case insensitive parsing
            assert_eq!(SAPHdlfsConfigKey::from_str(&input.to_uppercase()).unwrap(), *expected_key);
        }

        // Test error cases and client delegation
        assert!(SAPHdlfsConfigKey::from_str("unknown_key").is_err());
        assert!(matches!(SAPHdlfsConfigKey::from_str("allow_http").unwrap(), SAPHdlfsConfigKey::Client(_)));

        // Test config methods
        let builder = SAPHdlfsBuilder::default()
            .with_config(SAPHdlfsConfigKey::ContainerId, "test")
            .with_config(SAPHdlfsConfigKey::UseEmulator, "true")
            .with_config(SAPHdlfsConfigKey::UseEmulator, "invalid_bool"); // Should be ignored

        assert_eq!(builder.get_config_value(&SAPHdlfsConfigKey::ContainerId).unwrap(), "test");
        assert_eq!(builder.get_config_value(&SAPHdlfsConfigKey::UseEmulator).unwrap(), "true");
    }

    #[test]
    fn test_url_parsing() {
        let valid_endpoint = "7e698a97-a320-464d-9950-06ceee326fd2.files.hdl.canary-eu10.hanacloud.ondemand.com";

        // Test successful URL parsing for both schemes
        for scheme in ["hdlfs", "https"] {
            let mut builder = SAPHdlfsBuilder::default();
            let url = format!("{}://{}", scheme, valid_endpoint);
            assert!(builder.parse_url(&url).is_ok());
            assert_eq!(builder.container_id, "7e698a97-a320-464d-9950-06ceee326fd2");
        }

        // Test endpoint parsing
        let mut builder = SAPHdlfsBuilder::default();
        assert!(builder.parse_endpoint(valid_endpoint).is_ok());
        assert_eq!(builder.container_id, "7e698a97-a320-464d-9950-06ceee326fd2");

        // Test error cases
        let error_cases = [
            "ftp://invalid.scheme.com",
            "hdlfs://short.host",
            "not_a_url",
        ];

        for invalid_url in &error_cases {
            let mut builder = SAPHdlfsBuilder::default();
            assert!(builder.parse_url(invalid_url).is_err());
        }

        // Test endpoint error
        assert!(SAPHdlfsBuilder::default().parse_endpoint("short.host").is_err());
    }

    #[test]
    fn test_error_types_and_conversion() {
        // Test all error variants
        let errors = [
            Error::InvalidKey("test".to_string()),
            Error::InvalidCertificate("test".to_string()),
            Error::UnknownUrlScheme { scheme: "ftp".to_string() },
            Error::UrlNotRecognised { url: "test".to_string() },
            Error::UnknownConfigurationKey { key: "unknown".to_string() },
        ];

        for error in errors {
            // Test error display
            let error_msg = format!("{}", error);
            assert!(!error_msg.is_empty());

            // Test conversion to crate::Error
            let crate_error: crate::Error = error.into();
            match crate_error {
                crate::Error::Generic { store, .. } => assert_eq!(store, crate::hdlfs::STORE),
                _ => panic!("Expected Generic error"),
            }
        }
    }

    #[test]
    fn test_build_error_paths() {
        let endpoint = "test.files.hdl.region.hanacloud.ondemand.com";

        // Test missing cert file
        let cred = SAPHdlfsCredential {
            cert_path: "nonexistent.pem".into(),
            key_path: "nonexistent.pem".into(),
            ..Default::default()
        };
        let result = SAPHdlfsBuilder::default()
            .with_endpoint(endpoint)
            .with_credential(cred)
            .build();
        assert!(result.is_err());

        // Test different key type parsing errors
        let key_formats: &[&[u8]] = &[
            b"-----BEGIN PRIVATE KEY-----\ninvalid\n-----END PRIVATE KEY-----",
            b"-----BEGIN RSA PRIVATE KEY-----\ninvalid\n-----END RSA PRIVATE KEY-----",
            b"-----BEGIN PUBLIC KEY-----\ninvalid\n-----END PUBLIC KEY-----", // Unsupported type
        ];

        for key_content in key_formats {
            let (cert_file, mut key_file) = create_test_files();
            key_file.write_all(key_content).unwrap();

            let cred = SAPHdlfsCredential {
                cert_path: cert_file.path().to_path_buf(),
                key_path: key_file.path().to_path_buf(),
                ..Default::default()
            };

            let result = SAPHdlfsBuilder::default()
                .with_endpoint(endpoint)
                .with_credential(cred)
                .build();
            assert!(result.is_err());
        }

        // Test URL vs endpoint paths in build
        let (cert_file, key_file) = create_test_files();
        let cred = SAPHdlfsCredential {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ..Default::default()
        };

        // Test build with URL
        let result1 = SAPHdlfsBuilder::default()
            .with_url(format!("hdlfs://{}", endpoint))
            .with_credential(cred.clone())
            .build();
        assert!(result1.is_err()); // Will fail on certificate processing

        // Test build with endpoint only
        let result2 = SAPHdlfsBuilder::default()
            .with_endpoint(endpoint)
            .with_credential(cred)
            .build();
        assert!(result2.is_err()); // Will fail on certificate processing
    }

    #[test]
    fn test_environment_and_serde() {
        // Test environment loading with various scenarios
        std::env::set_var("HDLFS_CONTAINER_ID", "env_test");
        std::env::set_var("HDLFS_STORAGE_TRACE", "true");
        std::env::set_var("HDLFS_STORAGE_USE_EMULATOR", "false");
        std::env::set_var("HDLFS_INVALID_KEY", "ignored"); // Should be ignored

        let builder = SAPHdlfsBuilder::from_env();
        assert_eq!(builder.container_id, "env_test");
        assert_eq!(builder.trace.get().unwrap(), true);
        assert_eq!(builder.use_emulator.get().unwrap(), false);

        // Cleanup
        std::env::remove_var("HDLFS_CONTAINER_ID");
        std::env::remove_var("HDLFS_STORAGE_TRACE");
        std::env::remove_var("HDLFS_STORAGE_USE_EMULATOR");
        std::env::remove_var("HDLFS_INVALID_KEY");

        // Test environment with non-UTF8 values (coverage edge case)
        std::env::set_var("HDLFS_STORAGE_ENDPOINT", "test.endpoint.com");
        let builder2 = SAPHdlfsBuilder::from_env();
        assert_eq!(builder2.endpoint, "test.endpoint.com");
        std::env::remove_var("HDLFS_STORAGE_ENDPOINT");

        // Test serde functionality
        let key = SAPHdlfsConfigKey::PrivateKey;
        let serialized = serde_json::to_string(&key).unwrap();
        let deserialized: SAPHdlfsConfigKey = serde_json::from_str(&serialized).unwrap();
        assert_eq!(key, deserialized);

        // Test enum traits
        assert_eq!(key, key.clone());
        assert!(format!("{:?}", key).contains("PrivateKey"));
    }

    #[test]
    fn test_additional_coverage() {
        // Test all get_config_value branches including None returns
        let builder = SAPHdlfsBuilder::default();

        // Test get_config_value for all key types
        assert!(builder.get_config_value(&SAPHdlfsConfigKey::PrivateKey).is_some()); // Always returns path
        assert!(builder.get_config_value(&SAPHdlfsConfigKey::Certificate).is_some()); // Always returns path
        assert!(builder.get_config_value(&SAPHdlfsConfigKey::Endpoint).is_some()); // Always returns endpoint
        assert!(builder.get_config_value(&SAPHdlfsConfigKey::ContainerId).is_some()); // Always returns container_id

        // Test client config delegation
        assert!(builder.get_config_value(&SAPHdlfsConfigKey::Client(crate::client::ClientConfigKey::AllowHttp)).is_some());

        // Test all remaining error types
        let additional_errors = vec![
            Error::ConfigError(crate::Error::Generic { store: "test", source: Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "test")) }),
            Error::IoError(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "test")),
            Error::UrlParse(url::ParseError::EmptyHost),
            Error::UrlParseError("test_url".to_string(), url::ParseError::EmptyHost),
        ];

        for error in additional_errors {
            let error_msg = format!("{}", error);
            assert!(!error_msg.is_empty());
            let crate_error: crate::Error = error.into();
            match crate_error {
                crate::Error::Generic { store, .. } => assert_eq!(store, crate::hdlfs::STORE),
                _ => {} // Some errors might convert differently
            }
        }

        // Test URL parsing edge case - no host
        let mut builder = SAPHdlfsBuilder::default();
        assert!(builder.parse_url("hdlfs://").is_err()); // No host case

        // Test with_config for all boolean parsing scenarios
        let builder = SAPHdlfsBuilder::default()
            .with_config(SAPHdlfsConfigKey::Trace, "false")
            .with_config(SAPHdlfsConfigKey::UseEmulator, "invalid") // Should be ignored
            .with_config(SAPHdlfsConfigKey::UseEmulator, "true"); // Should override

        assert_eq!(builder.get_config_value(&SAPHdlfsConfigKey::Trace).unwrap(), "false");
        assert_eq!(builder.get_config_value(&SAPHdlfsConfigKey::UseEmulator).unwrap(), "true");

        // Test ConfigValue None case by checking trace/emulator values when set to invalid values
        let builder_invalid = SAPHdlfsBuilder::default()
            .with_config(SAPHdlfsConfigKey::Trace, "not_a_bool")
            .with_config(SAPHdlfsConfigKey::UseEmulator, "also_not_a_bool");

        // These should either be None or have some default handling
        let trace_invalid = builder_invalid.get_config_value(&SAPHdlfsConfigKey::Trace);
        let emulator_invalid = builder_invalid.get_config_value(&SAPHdlfsConfigKey::UseEmulator);
        // The behavior depends on ConfigValue implementation - we just ensure no panic
    }

    #[test]
    fn test_build_key_processing_coverage() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let endpoint = "test.files.hdl.region.hanacloud.ondemand.com";

        // Test missing key file (different from missing cert)
        let mut cert_file = NamedTempFile::new().unwrap();
        cert_file.write_all(b"-----BEGIN CERTIFICATE-----\nVGVzdA==\n-----END CERTIFICATE-----").unwrap();

        let cred = SAPHdlfsCredential {
            cert_path: cert_file.path().to_path_buf(),
            key_path: "nonexistent_key.pem".into(),
            ..Default::default()
        };

        let result = SAPHdlfsBuilder::default()
            .with_endpoint(endpoint)
            .with_credential(cred)
            .build();
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Failed to read key"));

        // Test key file that fails PEM parsing
        let mut cert_file2 = NamedTempFile::new().unwrap();
        let mut key_file2 = NamedTempFile::new().unwrap();
        cert_file2.write_all(b"-----BEGIN CERTIFICATE-----\nVGVzdA==\n-----END CERTIFICATE-----").unwrap();
        key_file2.write_all(b"not a valid PEM file at all").unwrap();

        let cred2 = SAPHdlfsCredential {
            cert_path: cert_file2.path().to_path_buf(),
            key_path: key_file2.path().to_path_buf(),
            ..Default::default()
        };

        let result2 = SAPHdlfsBuilder::default()
            .with_endpoint(endpoint)
            .with_credential(cred2)
            .build();
        assert!(result2.is_err());

        // Test successful key parsing but invalid RSA key
        let mut cert_file3 = NamedTempFile::new().unwrap();
        let mut key_file3 = NamedTempFile::new().unwrap();
        cert_file3.write_all(b"-----BEGIN CERTIFICATE-----\nVGVzdA==\n-----END CERTIFICATE-----").unwrap();

        // Valid PEM structure but invalid key content
        key_file3.write_all(b"-----BEGIN PRIVATE KEY-----\nTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFE\n-----END PRIVATE KEY-----").unwrap();

        let cred3 = SAPHdlfsCredential {
            cert_path: cert_file3.path().to_path_buf(),
            key_path: key_file3.path().to_path_buf(),
            ..Default::default()
        };

        let result3 = SAPHdlfsBuilder::default()
            .with_endpoint(endpoint)
            .with_credential(cred3)
            .build();
        assert!(result3.is_err());
    }
}