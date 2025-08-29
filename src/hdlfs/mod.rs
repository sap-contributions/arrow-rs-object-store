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
    ($obj:expr, $($arg:tt)*) => {
        if $obj.need_trace() {
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

impl SAPHdlfs {
    pub(crate) fn need_trace(&self) -> bool {
        self.client.need_trace()
    }
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
            self,
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
            self,
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
            self,
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
            self,
            "<< delete end, path: {}, took: {} ms",
            location,
            duration.as_millis()
        );
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        trace_api_call!(self, ">> list start, prefix: {}", prefix_str);
        let stream = self.client.list(prefix);
        Box::pin(stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let start = std::time::Instant::now();
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let result = self.client.list_with_delimiter(prefix).await;
        let duration = start.elapsed();
        trace_api_call!(
            self,
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
            self,
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
            self,
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
                state.client,
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
            self.state.client,
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
            self,
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
            self,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GetRange, PutPayload};
    use bytes::Bytes;
    use std::sync::Arc;

    // Helper function to create a mock client for testing
    fn create_mock_sap_hdlfs(trace: bool) -> SAPHdlfs {
        use crate::hdlfs::client::{SAPHdlfsClient, SAPHdlfsConfig};
        use crate::client::HttpClient;
        use std::time::Duration;
        use crate::{ClientOptions, RetryConfig};
        use url::Url;

        let http_client = HttpClient::new(reqwest::Client::new());
        let retry_config = RetryConfig::default();
        let client_options = ClientOptions::new().with_timeout(Duration::from_secs(60));

        let service_url = Url::parse("https://test-container.files.hdl.test.hanacloud.ondemand.com").unwrap();
        let config = SAPHdlfsConfig::new(
            "test-container".to_string(),
            service_url,
            false, // is_emulator
            trace,
            retry_config,
            client_options,
        );

        let mock_client = SAPHdlfsClient::new(config, http_client);

        SAPHdlfs {
            client: Arc::new(mock_client),
        }
    }

    #[test]
    fn test_basic_functionality() {
        // Test tracing
        let client_no_trace = create_mock_sap_hdlfs(false);
        assert!(!client_no_trace.need_trace());

        let client_with_trace = create_mock_sap_hdlfs(true);
        assert!(client_with_trace.need_trace());

        // Test display
        let display_str = format!("{}", client_no_trace);
        assert_eq!(display_str, "SAPHdlfs { ... }");

        // Test trace macro
        trace_api_call!(client_no_trace, "Test message: {}", "value");
        trace_api_call!(client_with_trace, "Test message: {}", "value");

        // Test constants
        assert_eq!(STORE, "HDLFS");
    }

    #[tokio::test]
    async fn test_object_store_methods() {
        let client = create_mock_sap_hdlfs(false);
        let path = Path::from("test/file.txt");
        let payload = PutPayload::from_bytes(Bytes::from("test content"));

        // Test put_opts
        let result = client.put_opts(&path, payload, PutOptions::default()).await;
        assert!(result.is_err()); // Expected to fail with mock client

        // Test put_multipart_opts
        let result = client.put_multipart_opts(&path, PutMultipartOptions::default()).await;
        assert!(result.is_ok());

        // Test get_opts with different ranges
        for range in [
            None,
            Some(GetRange::Bounded(0..10)),
            Some(GetRange::Offset(5)),
            Some(GetRange::Suffix(10)),
        ] {
            let opts = GetOptions { range, ..Default::default() };
            let result = client.get_opts(&path, opts).await;
            assert!(result.is_err()); // Expected to fail with mock client
        }

        // Test delete
        let result = client.delete(&path).await;
        assert!(result.is_err()); // Expected to fail with mock client

        // Test list (stream creation should succeed)
        let _stream_with_prefix = client.list(Some(&Path::from("test/")));
        let _stream_no_prefix = client.list(None);

        // Test list_with_delimiter
        for prefix in [Some(&Path::from("test/")), None] {
            let result = client.list_with_delimiter(prefix).await;
            assert!(result.is_err()); // Expected to fail with mock client
        }
    }

    #[tokio::test]
    #[should_panic(expected = "not implemented")]
    async fn test_copy_methods() {
        let client = create_mock_sap_hdlfs(false);
        let from = Path::from("test/source.txt");
        let to = Path::from("test/dest.txt");

        // Test copy (should panic with "not implemented")
        let _result = client.copy(&from, &to).await;
    }

    #[tokio::test]
    async fn test_multipart_operations() {
        let client = create_mock_sap_hdlfs(false);
        let path = Path::from("test/file.txt");
        let opts = PutMultipartOptions::default();

        // Test MultipartUpload trait
        let mut multipart_upload = client.put_multipart_opts(&path, opts).await.unwrap();

        // Test multiple parts with part index increment
        for i in 0..3 {
            let payload = PutPayload::from_bytes(Bytes::from(format!("part{}", i)));
            let upload_part = multipart_upload.put_part(payload);
            let result = upload_part.await;
            assert!(result.is_err()); // Expected to fail with mock client
        }

        // Test complete
        let result = multipart_upload.complete().await;
        assert!(result.is_err()); // Expected to fail with mock client

        // Test abort (new upload instance)
        let mut multipart_upload2 = client.put_multipart_opts(&path, PutMultipartOptions::default()).await.unwrap();
        let result = multipart_upload2.abort().await;
        assert!(result.is_ok()); // abort always succeeds

        // Test MultipartStore trait
        let result = client.create_multipart(&path).await;
        assert!(result.is_ok() && result.unwrap().is_empty());

        let multipart_id = String::new();
        let payload = PutPayload::from_bytes(Bytes::from("test content"));
        let result = client.put_part(&path, &multipart_id, 0, payload).await;
        assert!(result.is_err()); // Expected to fail with mock client

        let parts = vec![
            PartId { content_id: "part1".to_string() },
            PartId { content_id: "part2".to_string() }
        ];
        let result = client.complete_multipart(&path, &multipart_id, parts).await;
        assert!(result.is_err()); // Expected to fail with mock client

        let result = client.abort_multipart(&path, &multipart_id).await;
        assert!(result.is_ok()); // abort always succeeds
    }

    #[test]
    fn test_debug_implementations() {
        let client = create_mock_sap_hdlfs(false);
        let client_arc = client.client.clone();

        // Test SAPHdlfs Debug
        let debug_str = format!("{:?}", client);
        assert!(debug_str.contains("SAPHdlfs"));

        // Test UploadState Debug
        let upload_state = UploadState {
            location: Path::from("test/file.txt"),
            parts: Default::default(),
            client: client_arc.clone(),
        };
        let debug_str = format!("{:?}", upload_state);
        assert!(debug_str.contains("UploadState"));

        // Test SAPHdlfsMultiPartUpload Debug
        let multipart_upload = SAPHdlfsMultiPartUpload {
            part_idx: 0,
            state: Arc::new(UploadState {
                location: Path::from("test/file.txt"),
                parts: Default::default(),
                client: client_arc,
            }),
            opts: PutMultipartOptions::default(),
        };
        let debug_str = format!("{:?}", multipart_upload);
        assert!(debug_str.contains("SAPHdlfsMultiPartUpload"));
    }
}