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

//! An object store implementation for SAP HANA Cloud, Data Lake Files (hdlfs)

use crate::client::get::GetClientExt;
use crate::client::list::ListClientExt;
use crate::hdlfs::client::SAPHdlfsClient;
use crate::multipart::{MultipartStore, PartId};
use crate::{
    path::Path, GetOptions, GetRange, GetResult, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
};
use async_trait::async_trait;
pub use builder::SAPHdlfsBuilder;
pub use builder::SAPHdlfsConfigKey;
pub use credential::SAPHdlfsCredential;
use futures::stream::BoxStream;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

mod builder;
mod client;
mod credential;

mod list;
const STORE: &str = "HDLFS";

macro_rules! trace_api_call {
    ($cond:expr, $($arg:tt)*) => {
        if $cond {
            let thread_id = std::thread::current().id();
            eprintln!("[thread: {:>3?}] {}", thread_id, format!($($arg)*));
        }
    };
}

/// Client for SAP HANA Cloud, Data Lake Files operations
#[derive(Debug)]
pub struct SAPHdlfs {
    client: Arc<SAPHdlfsClient>,
}

#[async_trait]
impl ObjectStore for SAPHdlfs {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let start = std::time::Instant::now();
        let result = self.client.put_blob(location, payload, opts).await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< put_opts end, path: {}, took: {} ms",
            location,
            duration.as_millis()
        );
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let start = std::time::Instant::now();
        let result = Box::new(crate::hdlfs::SAPHdlfsMultiPartUpload {
            part_idx: 0,
            opts,
            state: Arc::new(UploadState {
                client: Arc::clone(&self.client),
                location: location.clone(),
                parts: Default::default(),
            }),
        });
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< put_multipart_opts end, path: {}, took: {} ms",
            location,
            duration.as_millis()
        );
        Ok(result)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let start = std::time::Instant::now();
        let length = match &options.range {
            Some(GetRange::Bounded(r)) => (r.end - r.start).to_string(),
            _ => "-".to_string(),
        };
        let result = self.client.get_opts(location, options).await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< get_opts end, path: {}, length: {:>8}, took: {} ms",
            location,
            length,
            duration.as_millis()
        );
        result
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let start = std::time::Instant::now();
        let result = self.client.delete_request(location, &()).await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< delete end, path: {}, took: {} ms",
            location,
            duration.as_millis()
        );
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        trace_api_call!(
            self.client.need_trace(),
            ">> list start, prefix: {}",
            prefix_str
        );
        let stream = self.client.list(prefix);
        Box::pin(stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let start = std::time::Instant::now();
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let result = self.client.list_with_delimiter(prefix).await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< list_with_delimiter end, prefix: {}, took: {} ms",
            prefix_str,
            duration.as_millis()
        );
        result
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let start = std::time::Instant::now();
        let result = self.client.copy_request(from, to, true).await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< copy end, from: {}, to: {}, took: {} ms",
            from,
            to,
            duration.as_millis()
        );
        result
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let start = std::time::Instant::now();
        let result = self.client.copy_request(from, to, false).await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< copy_if_not_exists end, from: {}, to: {}, took: {} ms",
            from,
            to,
            duration.as_millis()
        );
        result
    }
}

impl fmt::Display for SAPHdlfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SAPHdlfs {{ ... }}") // Customize as needed
    }
}

#[derive(Debug)]
struct SAPHdlfsMultiPartUpload {
    part_idx: usize,
    state: Arc<UploadState>,
    opts: PutMultipartOptions,
}

#[derive(Debug)]
struct UploadState {
    location: Path,
    parts: crate::client::parts::Parts,
    client: Arc<crate::hdlfs::client::SAPHdlfsClient>,
}

#[async_trait]
impl MultipartUpload for crate::hdlfs::SAPHdlfsMultiPartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let idx = self.part_idx;
        self.part_idx += 1;
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let start = std::time::Instant::now();
            let result = state.client.put_block(&state.location, idx, data).await;
            let duration = start.elapsed();
            trace_api_call!(
                state.client.need_trace(),
                "<< put_part end, path: {} part_idx: {}, took: {} ms",
                &state.location,
                idx,
                duration.as_millis()
            );
            let part = result?;
            state.parts.put(idx, part);
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let start = std::time::Instant::now();
        let parts = self.state.parts.finish(self.part_idx)?;
        let result = self
            .state
            .client
            .put_block_list(&self.state.location, parts, std::mem::take(&mut self.opts))
            .await;
        let duration = start.elapsed();
        trace_api_call!(
            self.state.client.need_trace(),
            "<< complete end, path: {} part_idx: {}, took: {} ms",
            &self.state.location,
            self.part_idx,
            duration.as_millis()
        );
        result
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl MultipartStore for crate::hdlfs::SAPHdlfs {
    async fn create_multipart(&self, _: &Path) -> Result<MultipartId> {
        Ok(String::new())
    }

    async fn put_part(
        &self,
        path: &Path,
        _: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        let start = std::time::Instant::now();
        let result = self.client.put_block(path, part_idx, data).await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< put_part end, path: {} part_idx: {}, took: {} ms",
            path,
            part_idx,
            duration.as_millis()
        );
        result
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        _: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let start = std::time::Instant::now();
        let result = self
            .client
            .put_block_list(path, parts, Default::default())
            .await;
        let duration = start.elapsed();
        trace_api_call!(
            self.client.need_trace(),
            "<< complete_multipart end, path: {}, took: {} ms",
            path,
            duration.as_millis()
        );
        result
    }

    async fn abort_multipart(&self, _: &Path, _: &MultipartId) -> Result<()> {
        Ok(())
    }
}
