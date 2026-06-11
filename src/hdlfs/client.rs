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

use crate::client::builder::HttpRequestBuilder;
use crate::client::retry::RetryExt;
use crate::client::HttpResponseBody;
use crate::path::Path;
use chrono::{TimeZone, Utc};
use hyper::{header::HeaderValue, StatusCode};
use url::Url;

use hyper::{header::CONTENT_LENGTH, header::CONTENT_RANGE, Method};

use crate::client::HttpClient;
use crate::PutPayload;
use crate::{GetRange, PutMode, PutMultipartOptions, PutOptions, PutResult};
use crate::{ListResult, ObjectMeta};

use crate::{ClientOptions, Result, RetryConfig};

use crate::client::HttpResponse;
use serde::Serialize;
use std::sync::Arc;

use crate::client::get::GetClient;
use crate::client::list::ListClient;
use crate::hdlfs::direct_access;
use crate::hdlfs::filestatus::FileStatusResponse;
use crate::hdlfs::list::{
    BatchDeleteWrapper, DeleteFile, DirectoryListing, MergeSource, MergeSourcesWrapper,
    NonRecursiveDirectoryListing,
};
use crate::list::{PaginatedListOptions, PaginatedListResult};
use crate::multipart::PartId;
use crate::GetOptions;
use async_trait::async_trait;

macro_rules! trace_log {
    ($client:expr, $($arg:tt)*) => {
        if $client.need_trace() {
            eprintln!($($arg)*);
        }
    }
}

const VERSION_HEADER: &str = "X-SAP-Generation";

const USER_DEFINED_METADATA_HEADER_PREFIX: &str = "X-SAP-";
const HDLFS_FILE_CONTAINER: &str = "X-SAP-FileContainer";
const HDLFS_CONTENT_TYPE: &str = "Content-Type";
const HDLFS_BINARY: &str = "application/octet-stream";
const HDLFS_JSON: &str = "application/json";

const LIST_DLT_SUFFIX1: &str = "__list_delta_table_1__/";
const LIST_DLT_SUFFIX2: &str = "__list_delta_table_2__/";
const LIST_FILES_SUFFIX3: &str = "__list_files_recursive__/";
const DELETE_FILES_RECURSIVE_SUFFIX: &str = "__delete_files_recursive__";
const DELETE_FILES_DIRECT_ACCESS_SUFFIX: &str = "__delete_files_direct_access__";
const READ_FILES_DIRECT_ACCESS_SUFFIX: &str = "__read_files_direct_access__";

/// Parsed result of a direct-access path suffix.
#[derive(Debug)]
struct DirectAccessRequest {
    /// The real folder path (without any suffix or duration).
    real_path: String,
    /// Privileges to request from GENERATE_TEMPORARY_CREDENTIALS.
    privileges: Vec<&'static str>,
    /// TTL in seconds to request; None means use server default (900 s).
    duration_seconds: Option<u64>,
}

/// Strip a direct-access suffix (and optional trailing digits encoding the
/// duration) from `path_str`.  Returns `None` when neither suffix matches.
fn parse_direct_access_path(path_str: &str) -> Option<DirectAccessRequest> {
    for (suffix, privileges) in [
        (DELETE_FILES_DIRECT_ACCESS_SUFFIX, vec!["BROWSE", "DELETE"]),
        (READ_FILES_DIRECT_ACCESS_SUFFIX, vec!["OPEN", "BROWSE"]),
    ] {
        // The path may end with the bare suffix, or with the suffix followed
        // by a decimal duration, e.g. "__delete_files_direct_access__3600".
        let (real_path, duration_seconds) = if let Some(stripped) = path_str.strip_suffix(suffix) {
            (stripped, None)
        } else if let Some(before) = path_str.rfind(suffix) {
            let tail = &path_str[before + suffix.len()..];
            if tail.chars().all(|c| c.is_ascii_digit()) && !tail.is_empty() {
                (&path_str[..before], tail.parse::<u64>().ok())
            } else {
                continue;
            }
        } else {
            continue;
        };

        return Some(DirectAccessRequest {
            real_path: real_path.trim_end_matches('/').to_owned(),
            privileges,
            duration_seconds,
        });
    }
    None
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ... other variants ...
    #[error("Error performing request {}: {}", path, source)]
    Request {
        source: crate::client::retry::RetryError,
        path: String,
    },

    #[error("Error performing get request {}: {}", path, source)]
    GetRequest {
        source: crate::client::retry::RetryError,
        path: String,
    },
    // ... other variants ...
    #[error("Got invalid list response: {}", source)]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[error("Got invalid list response (JSON): {source}")]
    InvalidListResponseJson { source: serde_json::Error },

    #[error("HTTP error: {source}")]
    Http {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Error performing list request: {}", source)]
    ListRequest {
        source: crate::client::retry::RetryError,
    },

    #[error("Not supported: {message}")]
    NotSupported { message: String },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path } => Self::Generic {
                store: crate::hdlfs::STORE,
                source: Box::new(Error::GetRequest { source, path }),
            },
            Error::Request { source, path } => Self::Generic {
                store: crate::hdlfs::STORE,
                source: Box::new(Error::Request { source, path }),
            },
            _ => Self::Generic {
                store: crate::hdlfs::STORE,
                source: Box::new(err),
            },
        }
    }
}

fn map_put_error(
    location: &Path,
    mode: PutMode,
    source: crate::client::retry::RetryError,
) -> crate::Error {
    use http::StatusCode;
    let path = location.as_ref().to_string();
    if matches!(mode, PutMode::Create) {
        let already_exists = source.status() == Some(StatusCode::CONFLICT)
            || source.status() == Some(StatusCode::PRECONDITION_FAILED)
            || source.body().is_some_and(direct_access::is_already_exists);
        if already_exists {
            return crate::Error::AlreadyExists {
                path,
                source: Box::new(source),
            };
        }
    }
    Error::Request { source, path }.into()
}

/// SAP HANA Cloud, Data Lake Files (hdlfs) client configuration
#[derive(Debug, Clone)]
pub(crate) struct SAPHdlfsConfig {
    pub container_id: String,
    pub retry_config: RetryConfig,
    pub service: Url,
    #[allow(dead_code)]
    pub is_emulator: bool,
    #[allow(dead_code)]
    pub trace: bool,
    /// Opt into direct-access redirects.
    pub direct_access: bool,
    #[allow(dead_code)]
    pub client_options: ClientOptions,
}

impl SAPHdlfsClient {
    pub(crate) fn with_config(client: HttpClient, config: SAPHdlfsConfig) -> Self {
        Self { client, config }
    }
}

impl SAPHdlfsConfig {
    /// Create new HDLFS configuration
    pub(crate) fn new(
        container_id: String,
        service: Url,
        is_emulator: bool,
        trace: bool,
        direct_access: bool,
        retry_config: RetryConfig,
        client_options: ClientOptions,
    ) -> Self {
        Self {
            container_id,
            retry_config,
            service,
            is_emulator,
            trace,
            direct_access,
            client_options,
        }
    }
}

/// A builder for a put request allowing customisation of the headers and query string
#[warn(dead_code)]
pub(crate) struct Request<'a> {
    path: &'a Path,
    config: &'a SAPHdlfsConfig,
    payload: Option<PutPayload>,
    builder: HttpRequestBuilder,
    #[allow(dead_code)]
    idempotent: bool,
}

impl Request<'_> {
    fn header(self, k: &str, v: &str) -> Self {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    #[allow(dead_code)]
    fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    fn with_payload(self, payload: PutPayload) -> Self {
        let content_length = payload.content_length();
        Self {
            builder: self.builder.header(CONTENT_LENGTH, content_length),
            payload: Some(payload),
            ..self
        }
    }

    fn with_extensions(self, extensions: ::http::Extensions) -> Self {
        let builder = self.builder.extensions(extensions);
        Self { builder, ..self }
    }

    async fn send(self) -> Result<HttpResponse, crate::client::retry::RetryError> {
        self.builder
            .retryable(&self.config.retry_config)
            .payload(self.payload)
            .send()
            .await
    }

    async fn do_put(self) -> Result<PutResult> {
        let path_str = self.path.as_ref().to_string();
        let response = self.send().await.map_err(move |source| Error::Request {
            source,
            path: path_str,
        })?;

        let e_tag = response
            .headers()
            .get("ETag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // HDLFS does not provide version or etag in headers, so return default PutResult
        Ok(PutResult {
            e_tag,
            version: None,
        })
    }
}

/// Client for HDLFS operations
#[derive(Debug, Clone)]
pub(crate) struct SAPHdlfsClient {
    config: SAPHdlfsConfig,
    client: HttpClient,
}

impl SAPHdlfsClient {
    /// Create a new [`SAPHdlfsClient`]
    #[allow(dead_code)]
    pub(crate) fn new(config: SAPHdlfsConfig, client: HttpClient) -> Self {
        Self { config, client }
    }

    pub(crate) fn object_url(&self, path: &Path) -> String {
        let double_encoded_path = path.as_ref().replace('%', "%25");
        self.config
            .service
            .join(&double_encoded_path)
            .unwrap()
            .to_string()
    }

    pub(crate) fn need_trace(&self) -> bool {
        self.config.trace
    }

    pub(crate) fn request<'a>(&'a self, method: Method, path: &'a Path) -> Request<'a> {
        let uri = self.object_url(path);
        let builder = self.client.request(method, uri);

        Request {
            path,
            builder,
            payload: None,
            config: &self.config,
            idempotent: false,
        }
    }

    pub(crate) async fn get_file_length(&self, path: &Path) -> crate::Result<u64> {
        let builder = self
            .request(Method::GET, path)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .query(&[("op", "GETFILESTATUS")]);

        let resp = match builder.send().await {
            Ok(r) => r,
            Err(source) => {
                if source.status() == Some(StatusCode::NOT_FOUND) {
                    return Err(crate::Error::NotFound {
                        path: path.as_ref().to_string(),
                        source: Box::new(source),
                    });
                }
                let path_str = path.as_ref().to_string();
                return Err(Error::GetRequest {
                    source,
                    path: path_str,
                }
                .into());
            }
        };

        let body = resp.into_body().bytes().await.map_err(|e| Error::Http {
            source: Box::new(e),
        })?;

        let meta: FileStatusResponse = serde_json::from_slice(&body)
            .map_err(|source| Error::InvalidListResponseJson { source })?;

        Ok(meta.file_status.length)
    }

    pub(crate) async fn put_blob(
        &self,
        location: &crate::path::Path,
        payload: crate::PutPayload,
        opts: crate::PutOptions,
    ) -> crate::Result<crate::PutResult> {
        let PutOptions {
            mode,
            // not supported by GCP
            tags: _,
            attributes: _,
            extensions,
        } = opts;
        let overwrite = !matches!(mode, PutMode::Create);
        let mut builder = self
            .request(Method::PUT, location)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .header(HDLFS_CONTENT_TYPE, HDLFS_BINARY)
            .query(&[
                ("op", "CREATE"),
                ("data", "true"),
                ("overwrite", if overwrite { "true" } else { "false" }),
            ])
            .with_payload(payload.clone())
            .with_extensions(extensions);
        if self.config.direct_access {
            builder = builder.header(direct_access::ACCEPT_HEADER, "true");
        }

        let path_str = location.as_ref().to_string();
        let response = match builder.send().await {
            Ok(r) => r,
            Err(source) => {
                return Err(map_put_error(location, mode, source));
            }
        };

        let response = if direct_access::is_redirect(&response) {
            direct_access::follow(
                &self.client,
                &self.config.retry_config,
                response,
                Some(payload),
                direct_access::ReplayOptions {
                    if_none_match: matches!(mode, PutMode::Create),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| match e {
                crate::Error::Precondition { source, .. } if matches!(mode, PutMode::Create) => {
                    crate::Error::AlreadyExists {
                        path: path_str.clone(),
                        source,
                    }
                }
                other => other,
            })?
        } else {
            response
        };

        let e_tag = response
            .headers()
            .get("ETag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // HDLFS does not provide version or etag in headers, so return default PutResult
        Ok(PutResult {
            e_tag,
            version: None,
        })
    }

    pub(crate) async fn put_block(
        &self,
        location: &crate::path::Path,
        part_idx: usize,
        payload: crate::PutPayload,
    ) -> Result<PartId> {
        let content_id = format!("tmp.{part_idx:x}");
        let path_str = location.as_ref().to_string() + "." + &content_id;
        let path = Path::from(path_str.clone());

        let builder = self
            .request(Method::PUT, &path)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .header(HDLFS_CONTENT_TYPE, HDLFS_BINARY)
            .query(&[("op", "CREATE"), ("data", "true"), ("overwrite", "true")]) // Adds ?op=CREATE&data=true
            .with_payload(payload);

        builder.do_put().await?;
        Ok(PartId { content_id })
    }

    pub(crate) async fn put_block_list(
        &self,
        location: &Path,
        parts: Vec<PartId>,
        _opts: PutMultipartOptions,
    ) -> Result<PutResult> {
        let target_path = location.as_ref();

        // Create the sources vector with relative paths (no leading slash)
        let sources: Vec<MergeSource> = parts
            .clone()
            .into_iter()
            .map(|part| MergeSource {
                path: format!("/{}.{}", target_path, part.content_id),
            })
            .collect();

        // Create the JSON wrapper object
        let merge_wrapper = MergeSourcesWrapper { sources };

        // Serialize to JSON without pretty printing
        let merge_json = serde_json::to_string(&merge_wrapper).unwrap();

        trace_log!(
            self,
            "phase 1: Merging parts into target path: {}",
            target_path
        );
        let builder = self
            .request(Method::POST, location)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .header(HDLFS_CONTENT_TYPE, HDLFS_JSON)
            .query(&[("op", "MERGE")])
            .with_payload(PutPayload::from_bytes(merge_json.into()));

        builder.do_put().await?;

        trace_log!(
            self,
            "phase 2: clean up temp files for target path: {}",
            target_path
        );

        // Create the sources vector with relative paths (no leading slash)
        let files: Vec<DeleteFile> = parts
            .clone()
            .into_iter()
            .map(|part| DeleteFile {
                path: format!("/{}.{}", target_path, part.content_id),
            })
            .collect();

        // Create the JSON wrapper object
        let delete_wrapper = BatchDeleteWrapper { files };

        // Serialize to JSON without pretty printing
        let delete_json = serde_json::to_string(&delete_wrapper).unwrap();

        let empty_path = &Path::from("");
        let builder = self
            .request(Method::POST, empty_path)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .header(HDLFS_CONTENT_TYPE, HDLFS_JSON)
            .query(&[("op", "DELETE_BATCH")])
            .with_payload(PutPayload::from_bytes(delete_json.into()));

        builder.do_put().await
    }

    pub(crate) async fn delete_request(
        &self,
        location: &crate::path::Path,
        _unused: &(),
    ) -> crate::Result<()> {
        let location_str = location.as_ref();

        // Check if the path has the recursive delete suffix
        let (actual_path, recursive) =
            if let Some(real_path) = location_str.strip_suffix(DELETE_FILES_RECURSIVE_SUFFIX) {
                trace_log!(
                    self,
                    "delete_request: deleting files recursively at path: {}",
                    real_path
                );
                (Path::from(real_path), true)
            } else {
                (location.clone(), false)
            };

        let query = if recursive {
            vec![("op", "DELETE"), ("recursive", "true")]
        } else {
            vec![("op", "DELETE")]
        };

        let builder = self
            .request(Method::DELETE, &actual_path)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .query(&query);

        let _ = builder.send().await.map_err(|source| {
            let path = actual_path.as_ref().into();
            Error::Request { source, path }
        })?;

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn list_with_delimiter(
        &self,
        _prefix: Option<&crate::path::Path>,
    ) -> crate::Result<crate::ListResult> {
        // empty body
        unimplemented!()
    }

    pub(crate) async fn copy_request(
        &self,
        _from: &crate::path::Path,
        _to: &crate::path::Path,
        _overwrite: bool,
    ) -> crate::Result<()> {
        // empty body
        unimplemented!()
    }

    async fn handle_chunked_response(
        &self,
        resp: HttpResponse,
        path: &Path,
    ) -> Result<(http::response::Parts, HttpResponseBody), crate::Error> {
        let (mut parts, body) = resp.into_parts();
        if parts.headers.get("transfer-encoding") == Some(&HeaderValue::from_static("chunked")) {
            let bytes = body.bytes().await.map_err(|e| Error::Http {
                source: Box::new(e),
            })?;
            let length = bytes.len();
            parts
                .headers
                .insert(CONTENT_LENGTH, length.to_string().parse().unwrap());
            parts.headers.remove("transfer-encoding");
            let body = HttpResponseBody::from(bytes);
            Ok((parts, body))
        } else {
            if parts.headers.get(CONTENT_LENGTH).is_none() {
                eprintln!(
                    "missing CONTENT_LENGTH path:{} header: {:?}",
                    path, parts.headers
                );
                let length = 0;
                parts.status = StatusCode::GATEWAY_TIMEOUT;
                parts
                    .headers
                    .insert(CONTENT_LENGTH, length.to_string().parse().unwrap());
            }
            Ok((parts, body))
        }
    }

    async fn list_delta_table(
        &self,
        path: &str,
        recursion_depth: usize,
    ) -> crate::Result<Vec<Path>> {
        let mut tab_list = Vec::new();
        let mut stack = vec![path.to_string()];

        while let Some(current_path) = stack.pop() {
            let current_path_obj = Path::from(current_path.clone());
            let nth_last_node = current_path
                .split('/')
                .filter(|s| !s.is_empty())
                .rev()
                .nth(recursion_depth - 1)
                .unwrap_or("");

            let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
            trace_log!(
                self,
                "list_delta_table, time:{} search path: {} recursion_depth: {} nth_last_node: {}",
                now,
                current_path,
                recursion_depth,
                nth_last_node
            );

            // Build the request with required headers and query
            let builder = self
                .request(Method::GET, &current_path_obj)
                .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
                .query(&[("op", "LISTSTATUS")]);

            let response = match builder.send().await {
                Ok(response) => response,
                Err(source) => {
                    trace_log!(self, "list_delta_table error: {}", source);
                    continue;
                }
            };

            let body_bytes = response
                .into_body()
                .bytes()
                .await
                .map_err(|e| Error::Http {
                    source: Box::new(e),
                })?;
            let dir_listing: NonRecursiveDirectoryListing = serde_json::from_slice(&body_bytes)
                .map_err(|source| Error::InvalidListResponseJson { source })?;

            // Extract the file status array from the nested structure
            dir_listing.file_statuses.file_status.iter().for_each(|f| {
                if f.file_type == "DIRECTORY" {
                    let child_path = f.path_suffix.strip_suffix('/').unwrap_or(&f.path_suffix);
                    let parent_path = current_path.strip_suffix('/').unwrap_or(&current_path);

                    if child_path == "_error_table_" {
                        return; // Skip "_error_table_" directories
                    }
                    if child_path == "_table_" {
                        let table_path = Path::from(format!("{}/{}/", parent_path, child_path));
                        tab_list.push(table_path);
                    } else {
                        let table_path =
                            Path::from(format!("{}/{}/_table_/", parent_path, child_path));
                        let next_path = format!("{}/{}/", parent_path, child_path).to_string();

                        if nth_last_node.len() <= 3 && nth_last_node.starts_with("v") {
                            tab_list.push(table_path);
                        } else {
                            stack.push(next_path);
                        }
                    }
                }
            });
        }
        Ok(tab_list)
    }

    async fn list_files_recursive(&self, path: &str) -> crate::Result<Vec<ObjectMeta>> {
        let mut objects = Vec::new();
        let mut start_after: Option<String> = None;
        let path_obj = Path::from(path);

        loop {
            let mut query = vec![("op", "LISTSTATUS_RECURSIVE")];
            if let Some(ref sa) = start_after {
                query.push(("startAfter", sa.as_str()));
            }

            let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
            trace_log!(
                self,
                "list_files_recursive, time:{} path: {} start_after: {:?}",
                now,
                path,
                start_after
            );

            // Build the request with required headers and query
            let builder = self
                .request(Method::GET, &path_obj)
                .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
                .query(&query);

            let response = match builder.send().await {
                Ok(response) => response,
                Err(source) => {
                    if source.status() == Some(StatusCode::NOT_FOUND) {
                        trace_log!(self, "list_files_recursive 404, path: {}", path);
                        break;
                    }
                    return Err(Error::ListRequest { source }.into());
                }
            };

            let body_bytes = response
                .into_body()
                .bytes()
                .await
                .map_err(|e| Error::Http {
                    source: Box::new(e),
                })?;

            let dir_listing: DirectoryListing = serde_json::from_slice(&body_bytes)
                .map_err(|source| Error::InvalidListResponseJson { source })?;

            // Extract files from the response
            let files = dir_listing
                .directory_listing
                .partial_listing
                .file_statuses
                .file_status;

            objects.extend(files.iter().filter_map(|f| {
                if f.file_type == "FILE" {
                    Some(ObjectMeta {
                        location: Path::from(format!("{}/{}", path, f.path_suffix)),
                        last_modified: Utc.timestamp_millis_opt(f.modification_time).single()?,
                        size: f.length,
                        e_tag: f.e_tag.clone(),
                        version: f.version.clone(),
                    })
                } else {
                    None
                }
            }));

            // Check if there are more pages
            let page_id = dir_listing.directory_listing.page_id.clone();
            if page_id.is_none() {
                break;
            }

            start_after = page_id;
        }

        Ok(objects)
    }
}

#[async_trait]
impl GetClient for SAPHdlfsClient {
    const STORE: &'static str = crate::hdlfs::STORE;
    const HEADER_CONFIG: crate::client::header::HeaderConfig =
        crate::client::header::HeaderConfig {
            etag_required: false,
            last_modified_required: false,
            version_header: Some(crate::hdlfs::client::VERSION_HEADER),
            user_defined_metadata_prefix: Some(
                crate::hdlfs::client::USER_DEFINED_METADATA_HEADER_PREFIX,
            ),
        };

    fn retry_config(&self) -> &RetryConfig {
        &self.config.retry_config
    }

    async fn get_request(
        &self,
        _ctx: &mut crate::client::retry::RetryContext,
        path: &Path,
        options: GetOptions,
    ) -> crate::Result<HttpResponse> {
        // Handle Direct Access credential vending requests
        let path_str = path.as_ref();
        if let Some(da) = parse_direct_access_path(path_str) {
            trace_log!(
                self,
                "[get_request]: direct access credential request — path: {}, privileges: {:?}, duration: {:?}",
                da.real_path, da.privileges, da.duration_seconds
            );

            let empty_path = &Path::from("");

            // Step 1: WHOAMI — gates the credential request on direct-access-prefix-mode-enabled.
            // X-SAP-FileContainer is required for authentication (server returns 401 without it).
            // If the HTTP call itself fails (old server, network error), fall through
            // non-fatally. If it succeeds but the flag is false/absent, return NotSupported.
            let whoami_builder = self
                .request(Method::GET, empty_path)
                .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
                .query(&[("op", "WHOAMI")]);

            // Response shape per spec:
            // { "user": "...", "options": [
            //     { "key": "direct-access-prefix-mode-enabled", "value": "true" },
            //     ...
            // ] }
            // options is an array of {key, value} objects; value is always a string.
            match whoami_builder.send().await {
                Ok(whoami_resp) => {
                    let enabled = whoami_resp
                        .into_body()
                        .bytes()
                        .await
                        .ok()
                        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                        .and_then(|j| {
                            j.get("options").and_then(|o| o.as_array()).map(|arr| {
                                arr.iter().any(|entry| {
                                    entry.get("key").and_then(|k| k.as_str())
                                        == Some("direct-access-prefix-mode-enabled")
                                        && entry.get("value").and_then(|v| v.as_str())
                                            == Some("true")
                                })
                            })
                        })
                        .unwrap_or(false);

                    trace_log!(
                        self,
                        "[get_request]: WHOAMI direct-access-prefix-mode-enabled={}",
                        enabled
                    );
                    if !enabled {
                        return Err(Error::NotSupported {
                            message: format!(
                                "HDLFS Direct Access (prefixModeEnabled) is not available for path '{}'. \
                                 Use delete_files() for namenode-based deletion.",
                                da.real_path
                            ),
                        }
                        .into());
                    }
                }
                Err(e) => {
                    trace_log!(self, "[get_request]: WHOAMI call failed (non-fatal): {}", e);
                }
            }

            // Step 2: GENERATE_TEMPORARY_CREDENTIALS — POST to root path with rules body.
            // The HDLFS response already contains everything Python needs (including
            // `type` per credential), so we forward the bytes verbatim — no parsing,
            // no injection, no re-serialisation.
            let mut req_body = serde_json::json!({
                "rules": [{
                    "paths": [format!("/{}", da.real_path)],
                    "privileges": da.privileges,
                }]
            });
            if let Some(dur) = da.duration_seconds {
                req_body["durationSeconds"] = serde_json::json!(dur);
            }
            let cred_json = serde_json::to_string(&req_body).unwrap();

            let cred_builder = self
                .request(Method::POST, empty_path)
                .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
                .header(HDLFS_CONTENT_TYPE, HDLFS_JSON)
                .query(&[("op", "GENERATE_TEMPORARY_CREDENTIALS")])
                .with_payload(PutPayload::from_bytes(cred_json.into()));

            let cred_resp = cred_builder.send().await.map_err(|source| {
                let path = path_str.to_string();
                Error::GetRequest { source, path }
            })?;

            let (_parts, resp_body) = self.handle_chunked_response(cred_resp, path).await?;
            let final_bytes = resp_body.bytes().await.map_err(|e| Error::Http {
                source: Box::new(e),
            })?;

            // GetClient calls get_request twice — once with head=true (size probe),
            // once with range=Some (read). Map both to the right HTTP shape:
            //   range=Some → 206 Partial Content + Content-Range + sliced body
            //   head=true  → 200 OK + Content-Length=total, empty body
            //   else       → 200 OK + full body
            let total = final_bytes.len();
            let (status, body, content_range, content_length) = match &options.range {
                Some(GetRange::Bounded(r)) => {
                    let (s, e) = (r.start as usize, (r.end as usize).min(total));
                    let cr = format!("bytes {}-{}/{}", s, e - 1, total);
                    let slice = final_bytes.slice(s..e);
                    let len = slice.len();
                    (StatusCode::PARTIAL_CONTENT, slice, Some(cr), len)
                }
                Some(GetRange::Offset(o)) => {
                    let s = *o as usize;
                    let cr = format!("bytes {}-{}/{}", s, total - 1, total);
                    let slice = final_bytes.slice(s..);
                    let len = slice.len();
                    (StatusCode::PARTIAL_CONTENT, slice, Some(cr), len)
                }
                Some(GetRange::Suffix(l)) => {
                    let s = total.saturating_sub(*l as usize);
                    let cr = format!("bytes {}-{}/{}", s, total - 1, total);
                    let slice = final_bytes.slice(s..);
                    let len = slice.len();
                    (StatusCode::PARTIAL_CONTENT, slice, Some(cr), len)
                }
                // Head request: report the TOTAL size, not the (empty) body size,
                // otherwise the client thinks the file is 0 bytes and skips the read.
                None if options.head => (StatusCode::OK, bytes::Bytes::new(), None, total),
                None => (StatusCode::OK, final_bytes, None, total),
            };
            let mut builder = http::Response::builder()
                .status(status)
                .header(CONTENT_LENGTH, content_length);
            if let Some(cr) = content_range {
                builder = builder.header(CONTENT_RANGE, cr);
            }
            return Ok(builder.body(HttpResponseBody::from(body)).unwrap());
        }

        let method = Method::GET;
        let mut parameters = vec![("op".to_owned(), "OPEN".to_owned())];

        // For metadata-only requests (no range) return file length without downloading the file content
        if options.range.is_none() && options.head {
            let file_len = self.get_file_length(path).await?;
            trace_log!(
                self,
                "[get_request]: metadata-only request for path: {}, length: {}",
                path,
                file_len
            );
            let response = http::Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, file_len)
                .body(HttpResponseBody::from(bytes::Bytes::new()))
                .unwrap();
            return Ok(response);
        }

        // Rust range syntax start..end is half‑open: it includes start and excludes end ([start, end)).
        // HTTP Content-Range uses inclusive bounds: bytes start-end/total
        // start = zero-based index of the first byte returned
        // end = zero-based index of the last byte returned (inclusive)
        // total = full size of the resource in bytes (or * if the total length is unknown)

        let mut content_range = String::new();
        let mut da_range_inclusive: Option<(u64, u64)> = None;
        if let Some(range) = &options.range {
            let file_len = self.get_file_length(path).await?;

            match range {
                GetRange::Bounded(r) => {
                    let offset = r.start.to_string();
                    let length = (r.end - r.start).to_string();
                    content_range = format!("bytes {}-{}/{}", r.start, r.end - 1, file_len);
                    da_range_inclusive = Some((r.start, r.end - 1));
                    parameters.push(("offset".to_owned(), offset));
                    parameters.push(("length".to_owned(), length));
                }
                GetRange::Offset(o) => {
                    let offset = o.to_string();
                    content_range = format!("bytes {}-/{}", offset, file_len);
                    da_range_inclusive = Some((*o, file_len.saturating_sub(1)));
                    parameters.push(("offset".to_owned(), offset));
                }
                GetRange::Suffix(l) => {
                    let start = file_len.saturating_sub(*l);
                    let end = file_len; // exclusive

                    let offset = start.to_string();
                    let length = l.to_string();
                    content_range = format!("bytes {}-{}/{}", start, end - 1, file_len);
                    da_range_inclusive = Some((start, end.saturating_sub(1)));
                    parameters.push(("offset".to_owned(), offset));
                    parameters.push(("length".to_owned(), length));
                }
            }
        }

        // Build the request with required headers and query
        let mut builder = self
            .request(method, path)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .query(&parameters);
        if self.config.direct_access {
            builder = builder.header(direct_access::ACCEPT_HEADER, "true");
        }

        let response = builder.send().await;

        match response {
            Ok(resp) => {
                let resp = if direct_access::is_redirect(&resp) {
                    direct_access::follow(
                        &self.client,
                        &self.config.retry_config,
                        resp,
                        None,
                        direct_access::ReplayOptions {
                            range: da_range_inclusive,
                            ..Default::default()
                        },
                    )
                    .await?
                } else {
                    resp
                };
                if options.range.is_some() {
                    let (mut parts, body) = self.handle_chunked_response(resp, path).await?;
                    parts
                        .headers
                        .insert(CONTENT_RANGE, content_range.parse().unwrap());
                    parts.status = StatusCode::PARTIAL_CONTENT;
                    let new_resp = HttpResponse::from_parts(parts, body);
                    Ok(new_resp)
                } else {
                    let (parts, body) = self.handle_chunked_response(resp, path).await?;
                    let new_resp = HttpResponse::from_parts(parts, body);
                    Ok(new_resp)
                }
            }
            Err(source) => {
                if source.status() == Some(StatusCode::NOT_FOUND) {
                    return Err(crate::Error::NotFound {
                        path: path.as_ref().to_string(),
                        source: Box::new(source),
                    });
                }
                let path = path.as_ref().to_string();
                Err(Error::GetRequest { source, path }.into())
            }
        }
    }
}

#[async_trait]
impl ListClient for Arc<SAPHdlfsClient> {
    async fn list_request(
        &self,
        prefix: Option<&str>,
        _opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let default_prefix = prefix.unwrap_or("/");
        let sub_path1 = default_prefix.strip_suffix(LIST_DLT_SUFFIX1);
        let sub_path2 = default_prefix.strip_suffix(LIST_DLT_SUFFIX2);
        let sub_path_recursive = default_prefix.strip_suffix(LIST_FILES_SUFFIX3);
        let path = Path::from(default_prefix);
        trace_log!(
            self,
            "list_request  default_prefix: {:?}  sub_path1: {:?}, sub_path2: {:?}, sub_path_recursive: {:?}",
            default_prefix,
            sub_path1,
            sub_path2,
            sub_path_recursive
        );

        // Check for recursive listing suffix first
        if let Some(recursive_path) = sub_path_recursive {
            let objects = self.list_files_recursive(recursive_path).await?;
            return Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: Vec::new(),
                    objects,
                },
                page_token: None,
            });
        }

        let (recursion_depth, list_path) = match (sub_path1, sub_path2) {
            (Some(path), _) => (1, path),
            (None, Some(path)) => (2, path),
            _ => (0, ""),
        };

        if recursion_depth > 0 {
            let common_prefixes = self.list_delta_table(list_path, recursion_depth).await?;
            return Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes,
                    objects: Vec::new(),
                },
                page_token: None,
            });
        }

        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let start_after: Option<String> = None;

        let mut query = vec![("op", "LISTSTATUS")];
        if let Some(ref sa) = start_after {
            query.push(("startAfter", sa.as_str()));
        }

        // Build the request with required headers and query
        let builder = self
            .request(Method::GET, &path)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .query(&query);

        let response = match builder.send().await {
            Ok(response) => response,
            Err(source) => {
                if source.status() == Some(StatusCode::NOT_FOUND) {
                    trace_log!(self, "ListClient 404 ");

                    // If the prefix does not exist, return an empty list.
                    return Ok(PaginatedListResult {
                        result: ListResult {
                            common_prefixes: Vec::new(),
                            objects: Vec::new(),
                        },
                        page_token: None,
                    });
                }
                return Err(Error::ListRequest { source }.into());
            }
        };

        let body_bytes = response
            .into_body()
            .bytes()
            .await
            .map_err(|e| Error::Http {
                source: Box::new(e),
            })?;

        // Print full response body for debugging
        let body_str = String::from_utf8_lossy(&body_bytes);
        trace_log!(
            self,
            "list_request , path:{:?} , full body len:{}",
            prefix,
            body_str.len()
        );

        // Parse the nested JSON structure
        let dir_listing: NonRecursiveDirectoryListing = serde_json::from_slice(&body_bytes)
            .map_err(|source| Error::InvalidListResponseJson { source })?;

        // Extract the file status array from the nested structure
        let files = dir_listing.file_statuses.file_status;

        objects.extend(files.iter().filter_map(|f| {
            if f.file_type == "FILE" {
                Some(ObjectMeta {
                    location: Path::from(format!("{}/{}", path.as_ref(), f.path_suffix)),
                    last_modified: Utc.timestamp_millis_opt(f.modification_time).single()?,
                    size: f.length,
                    e_tag: f.e_tag.clone(),
                    version: None,
                })
            } else {
                common_prefixes.push(Path::from(format!(
                    "{}/{}/",
                    path.as_ref(),
                    f.path_suffix.strip_suffix('/').unwrap_or(&f.path_suffix)
                )));
                None
            }
        }));

        // Only sort lexicographically when listing _delta_log directories.
        // Delta Lake's kernel expects files in lexicographic order: after finding a
        // checkpoint, it scans forward for subsequent commits. Without this sort,
        // the checkpoint.parquet (written last) can appear after the next commit's
        // .json in HDLFS's timestamp-based order, causing a version gap error.
        if default_prefix.contains("_delta_log") {
            trace_log!(
                self,
                "list_request: sorting {} objects lexicographically for _delta_log path: {:?}",
                objects.len(),
                default_prefix
            );
            objects.sort_by(|a, b| a.location.cmp(&b.location));
        }

        Ok(PaginatedListResult {
            result: ListResult {
                common_prefixes,
                objects,
            },
            page_token: None, // HDLFS does not support pagination, so we return None for next page token
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_put_error_uses_remote_exception() {
        // Smoke test: the `map_put_error` helper relies on
        // `direct_access::is_already_exists` for body inspection.
        // Detailed parsing tests live alongside that helper.
        assert!(direct_access::is_already_exists(
            r#"{"RemoteException":{"exception":"FileAlreadyExistsException"}}"#
        ));
    }
}
