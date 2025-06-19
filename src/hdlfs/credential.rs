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

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Certificate file not found: {0:?}")]
    CertificateNotFound(PathBuf),
    #[error("Private key file not found: {0:?}")]
    KeyNotFound(PathBuf),
}

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        Self::Generic {
            store: crate::hdlfs::STORE,
            source: Box::new(value),
        }
    }
}

/// Holds the certificate and private key paths for HDLFS authentication.
///
/// This struct is used to provide the necessary credentials (certificate and key)
/// required to authenticate with SAP HANA Cloud, Data Lake Files (HDLFS).
#[derive(Default, Debug, Clone)]
pub struct SAPHdlfsCredential {
    /// Path to the client certificate file.
    pub cert_path: PathBuf,
    /// Path to the private key file.
    pub key_path: PathBuf,
}

impl SAPHdlfsCredential {
    /// Creates a new `HdlfsCredential` with the given certificate and key paths.
    ///
    /// # Arguments
    ///
    /// * `cert_path` - Path to the client certificate file.
    /// * `key_path` - Path to the private key file.
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }

    /// Validates that the certificate and key files exist at the specified paths.
    ///
    /// Returns an error if either file is missing.
    pub fn validate(&self) -> Result<(), Error> {
        if !self.cert_path.exists() {
            return Err(Error::CertificateNotFound(self.cert_path.clone()));
        }
        if !self.key_path.exists() {
            return Err(Error::KeyNotFound(self.key_path.clone()));
        }
        Ok(())
    }
}
