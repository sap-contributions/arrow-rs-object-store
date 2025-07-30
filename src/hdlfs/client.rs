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
const MAX_OFFSET: &i64 = &(1024 * 1024 * 1024 * 16); // 16 GB

const LIST_DLT_SUFFIX: &str = "__list_delta_table__/";

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
        retry_config: RetryConfig,
        client_options: ClientOptions,
    ) -> Self {
        Self {
            container_id,
            retry_config,
            service,
            is_emulator,
            trace,
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
            e_tag: e_tag,
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
        self.config.service.join(path.as_ref()).unwrap().to_string()
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
        let builder = self
            .request(Method::PUT, location)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .header(HDLFS_CONTENT_TYPE, HDLFS_BINARY)
            .query(&[("op", "CREATE"), ("data", "true")]) // Adds ?op=CREATE&data=true
            .with_payload(payload)
            .with_extensions(extensions);

        match (mode, builder.do_put().await) {
            (PutMode::Create, Err(crate::Error::Precondition { path, source })) => {
                Err(crate::Error::AlreadyExists { path, source })
            }
            (_, r) => r,
        }
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

        Ok(builder.do_put().await?)
    }

    pub(crate) async fn delete_request(
        &self,
        location: &crate::path::Path,
        _unused: &(),
    ) -> crate::Result<()> {
        let builder = self
            .request(Method::DELETE, location)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .query(&[("op", "DELETE")]);

        let _ = builder.send().await.map_err(|source| {
            let path = location.as_ref().into();
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

    async fn list_delta_table(&self, path: &str) -> crate::Result<Vec<Path>> {
        let mut tab_list = Vec::new();
        let mut stack = vec![path.to_string()];

        while let Some(current_path) = stack.pop() {
            trace_log!(self, "list_delta_table, search path: {}", current_path);

            let current_path_obj = Path::from(current_path.clone());
            let leaf_node = current_path
                .split('/')
                .filter(|s| !s.is_empty())
                .last()
                .unwrap_or("");

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

            // Print full response body for debugging
            let body_str = String::from_utf8_lossy(&body_bytes);

            // Parse the nested JSON structure
            let dir_listing: NonRecursiveDirectoryListing = serde_json::from_slice(&body_bytes)
                .map_err(|source| Error::InvalidListResponseJson { source })?;

            // Extract the file status array from the nested structure
            dir_listing.file_statuses.file_status.iter().for_each(|f| {
                if f.file_type == "DIRECTORY" {
                    let child_path = f
                        .path_suffix
                        .strip_suffix('/')
                        .unwrap_or(&f.path_suffix)
                        .clone();
                    let parent_path = current_path
                        .strip_suffix('/')
                        .unwrap_or(&current_path)
                        .clone();
                    let table_path = Path::from(format!("{}/{}/_table_/", parent_path, child_path));
                    let next__path = format!("{}/{}/", parent_path, child_path).to_string();

                    if leaf_node.len() <= 3 && leaf_node.starts_with("v") {
                        tab_list.push(table_path);
                    } else {
                        stack.push(next__path);
                    }
                }
            });
        }
        Ok(tab_list)
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
        let method = Method::GET;

        let mut parameters = vec![("op".to_owned(), "OPEN".to_owned())];

        let mut content_range = String::new();
        if let Some(range) = &options.range {
            match range {
                GetRange::Bounded(r) => {
                    let offset = r.start.to_string();
                    let length = (r.end - r.start).to_string();
                    content_range = format!("bytes {}-{}/{}", r.start, r.end - 1, MAX_OFFSET);
                    parameters.push(("offset".to_owned(), offset));
                    parameters.push(("length".to_owned(), length));
                }
                GetRange::Offset(o) => {
                    let offset = o.to_string();
                    content_range = format!("bytes {}-/*", offset);
                    parameters.push(("offset".to_owned(), offset));
                }
                GetRange::Suffix(l) => {
                    let length = l.to_string();
                    content_range = format!("bytes -{}/*", length);
                    parameters.push(("length".to_owned(), length));
                }
            }
        }

        // Build the request with required headers and query
        let builder = self
            .request(method, path)
            .header(HDLFS_FILE_CONTAINER, &self.config.container_id)
            .query(&parameters);

        let builder = builder;
        let response = builder.send().await;

        match response {
            Ok(resp) => {
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
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let default_prefix = prefix.unwrap_or("/");
        let sub_path = default_prefix.strip_suffix(LIST_DLT_SUFFIX);
        let path = Path::from(default_prefix);
        trace_log!(
            self,
            "list_request  default_prefix: {:?}  suffix:{} sub_path: {:?}",
            default_prefix,
            LIST_DLT_SUFFIX,
            sub_path
        );

        if sub_path.is_some() {
            let common_prefixes = self.list_delta_table(sub_path.unwrap()).await?;
            return Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: common_prefixes,
                    objects: Vec::new(),
                },
                page_token: None,
            });
        }

        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut start_after: Option<String> = None;
        let mut loop_count = 0;

        loop {
            let mut query = vec![("op", "LISTSTATUS_RECURSIVE")];
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
            let dir_listing: DirectoryListing = serde_json::from_slice(&body_bytes)
                .map_err(|source| Error::InvalidListResponseJson { source })?;

            trace_log!(
                self,
                "list_request loop_count:{}, page_id: {:?}",
                loop_count,
                dir_listing.directory_listing.page_id
            );

            // Extract the file status array from the nested structure
            let files = dir_listing
                .directory_listing
                .partial_listing
                .file_statuses
                .file_status;

            objects.extend(files.iter().filter_map(|f| {
                if f.file_type == "FILE" {
                    Some(ObjectMeta {
                        location: Path::from(format!("{}/{}", path.as_ref(), f.path_suffix)),
                        last_modified: Utc.timestamp_millis_opt(f.modification_time).single()?,
                        size: f.length,
                        e_tag: f.e_tag.clone(),
                        version: f.version.clone(),
                    })
                } else {
                    None
                }
            }));

            common_prefixes.extend(files.iter().filter_map(|f| {
                if f.file_type == "DIRECTORY" {
                    Some(Path::from(format!("{}/{}", path.as_ref(), f.path_suffix)))
                } else {
                    None
                }
            }));

            // Get the next page_id
            let page_id = dir_listing.directory_listing.page_id.clone();
            if page_id.is_none() {
                break;
            }

            start_after = page_id;
            loop_count += 1;
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
