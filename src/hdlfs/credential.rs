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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

    #[test]
    fn test_credential_new_and_fields() {
        let cert = PathBuf::from("cert.pem");
        let key = PathBuf::from("key.pem");
        let cred = SAPHdlfsCredential::new(&cert, &key);
        assert_eq!(cred.cert_path, cert);
        assert_eq!(cred.key_path, key);
    }

    #[test]
    fn test_validate_missing_files() {
        let cert = PathBuf::from("missing_cert.pem");
        let key = PathBuf::from("missing_key.pem");
        let cred = SAPHdlfsCredential::new(&cert, &key);
        let err = cred.validate().unwrap_err();
        match err {
            Error::CertificateNotFound(p) => assert_eq!(p, cert),
            _ => panic!("Expected CertificateNotFound"),
        }
    }

    #[test]
    fn test_validate_key_missing() {
        let cert = env::temp_dir().join("test_cert.pem");
        fs::File::create(&cert).unwrap();
        let key = PathBuf::from("missing_key.pem");
        let cred = SAPHdlfsCredential::new(&cert, &key);
        let err = cred.validate().unwrap_err();
        match err {
            Error::KeyNotFound(p) => assert_eq!(p, key),
            _ => panic!("Expected KeyNotFound"),
        }
        let _ = fs::remove_file(&cert);
    }

    #[test]
    fn test_validate_success() {

        let cert = env::temp_dir().join("test_cert_validate.pem");
        let key = env::temp_dir().join("test_key_validate.pem");

        // Create files with content to ensure they exist
        use std::io::Write;
        let mut cert_file = fs::File::create(&cert).expect("Failed to create cert file");
        cert_file.write_all(b"dummy cert content").expect("Failed to write cert");
        cert_file.sync_all().expect("Failed to sync cert");
        drop(cert_file);

        let mut key_file = fs::File::create(&key).expect("Failed to create key file");
        key_file.write_all(b"dummy key content").expect("Failed to write key");
        key_file.sync_all().expect("Failed to sync key");
        drop(key_file);

        // Double check files exist
        assert!(cert.exists(), "Cert file does not exist after creation");
        assert!(key.exists(), "Key file does not exist after creation");

        let cred = SAPHdlfsCredential::new(&cert, &key);
        assert!(cred.validate().is_ok());

        // Cleanup
        let _ = fs::remove_file(&cert);
        let _ = fs::remove_file(&key);
    }

    #[test]
    fn test_error_conversion() {
        let cert = PathBuf::from("missing_cert.pem");
        let err = Error::CertificateNotFound(cert.clone());
        let crate_err: crate::Error = err.into();
        match crate_err {
            crate::Error::Generic { store, source } => {
                assert_eq!(store, crate::hdlfs::STORE);
                assert!(format!("{:?}", source).contains("CertificateNotFound"));
            }
            _ => panic!("Expected Generic error"),
        }
    }
}