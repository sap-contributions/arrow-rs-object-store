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

//! Direct-access redirect handling.

use std::collections::HashMap;
use std::str::FromStr;

use http::header::CONTENT_LENGTH;
use hyper::Method;
use serde::Deserialize;

use crate::client::retry::RetryExt;
use crate::client::{HttpClient, HttpResponse};
use crate::{PutPayload, RetryConfig};

pub(crate) const ACCEPT_HEADER: &str = "X-SAP-Accept-Direct-Access";
pub(crate) const RESPONSE_HEADER: &str = "X-SAP-Direct-Access";
pub(crate) const STORE: &str = crate::hdlfs::STORE;

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct ReplayOptions {
    pub range: Option<(u64, u64)>,
    pub if_none_match: bool,
}

pub(crate) fn is_redirect(response: &HttpResponse) -> bool {
    response.status().is_success()
        && response
            .headers()
            .get(RESPONSE_HEADER)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

pub(crate) async fn follow(
    client: &HttpClient,
    retry: &RetryConfig,
    response: HttpResponse,
    payload: Option<PutPayload>,
    opts: ReplayOptions,
) -> crate::Result<HttpResponse> {
    let body = response
        .into_body()
        .bytes()
        .await
        .map_err(|e| descriptor_error(format!("read body: {e}")))?;
    let descriptor: DirectAccessResponse = serde_json::from_slice(&body)
        .map_err(|e| descriptor_error(format!("invalid descriptor JSON: {e}")))?;
    let method = Method::from_str(&descriptor.properties.method)
        .map_err(|e| descriptor_error(format!("invalid method: {e}")))?;

    let mut redirect = client.request(method, descriptor.properties.endpoint);
    for (key, value) in descriptor.properties.headers {
        redirect = redirect.header(key.as_str(), value.as_str());
    }
    if let Some(ref p) = payload {
        redirect = redirect.header(CONTENT_LENGTH, p.content_length());
    }
    if let Some((start, end_inclusive)) = opts.range {
        redirect = redirect.header("Range", &format!("bytes={start}-{end_inclusive}"));
    }
    if opts.if_none_match {
        redirect = redirect.header("If-None-Match", "*");
    }

    redirect
        .retryable(retry)
        .payload(payload)
        .send()
        .await
        .map_err(|source| source.error(STORE, String::new()))
}

#[derive(Deserialize, Debug)]
struct RemoteExceptionEnvelope {
    #[serde(rename = "RemoteException")]
    remote_exception: RemoteException,
}

#[derive(Deserialize, Debug)]
struct RemoteException {
    #[serde(default)]
    exception: String,
    #[serde(default, rename = "javaClassName")]
    java_class_name: String,
}

pub(crate) fn is_already_exists(body: &str) -> bool {
    if let Ok(env) = serde_json::from_str::<RemoteExceptionEnvelope>(body) {
        let exc = &env.remote_exception;
        return exc.exception == "FileAlreadyExistsException"
            || exc.java_class_name.ends_with("FileAlreadyExistsException");
    }
    body.contains("FileAlreadyExistsException")
}

#[derive(Deserialize, Debug)]
struct DirectAccessProperties {
    endpoint: String,
    method: String,
    #[serde(default)]
    headers: HashMap<String, String>,
}

#[derive(Deserialize, Debug)]
struct DirectAccessResponse {
    properties: DirectAccessProperties,
}

fn descriptor_error(message: String) -> crate::Error {
    crate::Error::Generic {
        store: STORE,
        source: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_descriptor_with_headers() {
        let json = r#"{
            "properties": {
                "endpoint": "https://example.invalid/key?sig=...",
                "method": "GET",
                "headers": { "x-custom": "v" }
            }
        }"#;
        let d: DirectAccessResponse = serde_json::from_str(json).unwrap();
        assert_eq!(d.properties.method, "GET");
        assert_eq!(d.properties.headers.len(), 1);
    }

    #[test]
    fn descriptor_headers_default_to_empty() {
        let json = r#"{"properties":{"endpoint":"https://x/y","method":"PUT"}}"#;
        let d: DirectAccessResponse = serde_json::from_str(json).unwrap();
        assert!(d.properties.headers.is_empty());
    }

    #[test]
    fn already_exists_typed_envelope() {
        let body =
            r#"{"RemoteException":{"exception":"FileAlreadyExistsException","message":"..."}}"#;
        assert!(is_already_exists(body));
    }

    #[test]
    fn already_exists_java_class_name() {
        let body = r#"{"RemoteException":{"exception":"X","javaClassName":"org.apache.hadoop.fs.FileAlreadyExistsException"}}"#;
        assert!(is_already_exists(body));
    }

    #[test]
    fn already_exists_unstructured_body() {
        assert!(is_already_exists("FileAlreadyExistsException: /foo"));
    }

    #[test]
    fn already_exists_does_not_match_unrelated_403() {
        assert!(!is_already_exists("Forbidden"));
        assert!(!is_already_exists(
            r#"{"RemoteException":{"exception":"AccessControlException"}}"#
        ));
    }
}
