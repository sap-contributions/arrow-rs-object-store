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

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub(crate) struct DirectoryListing {
    #[serde(rename = "DirectoryListing")]
    pub directory_listing: PartialListing,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PartialListing {
    #[serde(rename = "partialListing")]
    pub partial_listing: FileStatusesWrapper,
}

#[derive(Debug, Deserialize)]
pub(super) struct FileStatusesWrapper {
    #[serde(rename = "FileStatuses")]
    pub file_statuses: FileStatuses,
}

#[derive(Debug, Deserialize)]
pub(super) struct FileStatuses {
    #[serde(rename = "FileStatus")]
    pub file_status: Vec<FileStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct FileStatus {
    pub replication: u32,
    pub owner: String,
    pub length: u64,
    pub permission: String,
    #[serde(rename = "type")]
    pub file_type: String,
    pub version: Option<String>,
    pub block_size: u64,
    pub path_suffix: String,
    pub is_deleted: bool,
    pub modification_time: i64,
    pub e_tag: Option<String>,
    pub access_time: i64,
    pub group: String,
}

#[derive(Serialize, Deserialize)]
pub(super) struct MergeSource {
    pub path: String,
}

#[derive(Serialize, Deserialize)]
pub(super) struct MergeSourcesWrapper {
    pub sources: Vec<MergeSource>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct DeleteFile {
    pub path: String,
}

#[derive(Serialize, Deserialize)]
pub(super) struct BatchDeleteWrapper {
    pub files: Vec<DeleteFile>,
}
