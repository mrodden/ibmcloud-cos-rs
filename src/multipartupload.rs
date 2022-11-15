// Copyright 2022 Mathew Odden <mathewrodden@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use quick_xml::{de::from_str, se::to_string};
use reqwest::blocking::Body;
use serde::{Deserialize, Serialize};

use crate::cos::{check_response, Client, Error};

#[derive(Deserialize, Debug)]
pub struct InitiateMultipartUploadResult {
    #[serde(rename = "$unflatten=Bucket")]
    pub bucket: String,
    #[serde(rename = "$unflatten=Key")]
    pub key: String,
    #[serde(rename = "$unflatten=UploadId")]
    pub upload_id: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Part {
    #[serde(rename = "$unflatten=ETag")]
    pub etag: String,
    #[serde(rename = "$unflatten=PartNumber")]
    pub part_number: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CompleteMultipartUpload {
    #[serde(rename = "Part", default)]
    pub parts: Vec<Part>,
}

pub type UploadId = String;

impl Client {
    pub fn create_multipart_upload(&self, bucket: &str, key: &str) -> Result<UploadId, Error> {
        let c = &self.client;

        let url = format!("https://{}.{}/{}?uploads", bucket, self.endpoint, key);
        let response = c
            .post(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .send()?;

        let text: String = check_response(response)?.text()?;
        let mpu_resp: InitiateMultipartUploadResult = from_str(&text)?;

        Ok(mpu_resp.upload_id)
    }

    pub fn upload_part<T: Into<Body>>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        sequence_number: usize,
        chunk: T,
    ) -> Result<Part, Error> {
        let c = &self.client;

        let url = format!(
            "https://{}.{}/{}?partNumber={}&uploadId={}",
            bucket, self.endpoint, key, sequence_number, upload_id,
        );

        let resp = c
            .put(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .body(chunk)
            .send()?;

        let resp = check_response(resp)?;
        let etag = resp.headers()[reqwest::header::ETAG].to_str().unwrap();

        let part = Part {
            etag: etag.to_string(),
            part_number: sequence_number,
        };

        Ok(part)
    }

    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        cmpu: CompleteMultipartUpload,
    ) -> Result<(), Error> {
        let c = &self.client;

        let url = format!(
            "https://{}.{}/{}?uploadId={}",
            bucket, self.endpoint, key, upload_id
        );

        let payload = to_string(&cmpu).unwrap();

        let resp = c
            .post(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .body(payload)
            .send()?;

        let _ = check_response(resp)?;

        Ok(())
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), Error> {
        let c = &self.client;

        let url = format!(
            "https://{}.{}/{}?uploadId={}",
            bucket, self.endpoint, key, upload_id
        );

        let resp = c
            .delete(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .send()?;

        let _ = check_response(resp)?;

        Ok(())
    }
}
