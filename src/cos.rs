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

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;

use ibmcloud_iam::token::TokenManager;
use quick_xml::de::from_str;
use reqwest;
use serde;
use serde::{Deserialize, Serialize};
use tracing::error;

pub type Error = Box<dyn std::error::Error>;

#[derive(Deserialize, Serialize, Debug)]
pub struct ListAllMyBucketsResult {
    #[serde(rename = "Owner")]
    owner: Owner,
    #[serde(rename = "Buckets")]
    buckets: Buckets,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Buckets {
    #[serde(rename = "Bucket")]
    list: Vec<Bucket>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Owner {
    #[serde(rename = "$unflatten=ID")]
    id: String,
    #[serde(rename = "$unflatten=DisplayName")]
    display_name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Bucket {
    #[serde(rename = "$unflatten=Name")]
    pub name: String,
    #[serde(rename = "$unflatten=CreationDate")]
    pub creation_date: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListBucketResult {
    #[serde(rename = "Contents")]
    contents: Vec<Contents>,
    #[serde(rename = "$unflatten=KeyCount")]
    key_count: u64,
    #[serde(rename = "$unflatten=MaxKeys")]
    max_keys: u64,
    #[serde(rename = "$unflatten=NextContinuationToken")]
    next_token: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Contents {
    #[serde(rename = "$unflatten=Key")]
    pub key: String,
    #[serde(rename = "$unflatten=LastModified")]
    pub last_modified: String,
    #[serde(rename = "$unflatten=ETag")]
    pub etag: String,
    #[serde(rename = "$unflatten=Size")]
    pub size: u64,
    #[serde(rename = "$unflatten=StorageClass")]
    pub storage_class: String,
}

pub struct Client {
    pub(crate) tm: Arc<TokenManager>,
    pub(crate) endpoint: String,
    pub(crate) client: reqwest::blocking::Client,
}

impl Client {
    pub fn new(tm: Arc<TokenManager>, endpoint: &str) -> Self {
        Self {
            tm: tm,
            endpoint: endpoint.to_string(),
            client: reqwest::blocking::Client::new(),
        }
    }

    pub fn list_buckets(&self, instance_id: &str) -> Result<Vec<Bucket>, Error> {
        let c = &self.client;

        let url = format!("https://{}/", self.endpoint);
        let response = c
            .get(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .header("ibm-service-instance-id", instance_id.to_string())
            .send()?;

        let text: String = check_response(response)?.text()?;
        let bucket_resp: ListAllMyBucketsResult = from_str(&text)?;

        Ok(bucket_resp.buckets.list)
    }

    pub fn list_objects(&self, bucket: &str, prefix: Option<String>, start_after: Option<String>) -> ObjectIterator {
        ObjectIterator::new(self, bucket, prefix.clone(), start_after.clone())
    }

    fn _list_objects(
        &self,
        bucket: &str,
        prefix: &Option<String>,
        start_after: &Option<String>,
        continuation_token: &Option<String>,
    ) -> Result<ListBucketResult, Error> {
        let c = &self.client;

        let mut url = format!("https://{}.{}/?list-type=2", bucket, self.endpoint);

        if let Some(tok) = continuation_token {
            url = format!("{}&continuation-token={}", url, tok);
        }

        if let Some(pre) = prefix {
            url = format!("{}&prefix={}", url, pre);
        }

        if let Some(after) = start_after {
            url = format!("{}&start-after={}", url, after);
        }

        let response = c
            .get(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .send()?;

        let text: String = check_response(response)?.text()?;
        let objlist: ListBucketResult = match from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                let s = format!("{}", e);
                if s.contains("missing field `Contents`") {
                    return Err("No contents in bucket".into());
                } else {
                    return Err(Box::new(e));
                }
            }
        };

        Ok(objlist)
    }

    pub fn get_object_at_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn Read>, Error> {
        let c = &self.client;
        let url = format!("https://{}.{}/{}", bucket, self.endpoint, key);

        let mut end_str = "".to_string();
        if let Some(e) = end {
            end_str = format!("{}", e);
        }

        let response = c
            .get(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .header("Range", format!("bytes={}-{}", start, end_str))
            .send()?;

        let r = check_response(response)?;
        Ok(Box::new(r))
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Box<dyn Read>, Error> {
        let c = &self.client;
        let url = format!("https://{}.{}/{}", bucket, self.endpoint, key);

        let response = c
            .get(url)
            .header(
                "Authorization",
                format!("Bearer {}", self.tm.token()?.access_token),
            )
            .send()?;

        let r = check_response(response)?;
        Ok(Box::new(r))
    }
}

pub(crate) fn check_response(
    response: reqwest::blocking::Response,
) -> Result<reqwest::blocking::Response, Error> {
    if !response.status().is_success() {
        return Err(format!(
            "request failed: code='{}' body='{:?}'",
            response.status(),
            response.text().unwrap()
        )
        .into());
    }

    Ok(response)
}

pub struct ObjectIterator<'a> {
    client: &'a Client,
    bucket: String,
    prefix: Option<String>,
    continuation_token: Option<String>,
    start_after: Option<String>,
    results: VecDeque<Contents>,
    complete: bool,
}

impl<'a> ObjectIterator<'a> {
    pub fn new(client: &'a Client, bucket: &str, prefix: Option<String>, start_after: Option<String>) -> Self {
        Self {
            client,
            bucket: bucket.to_string(),
            prefix: prefix,
            continuation_token: None,
            start_after: start_after,
            results: VecDeque::new(),
            complete: false,
        }
    }
}

impl Iterator for ObjectIterator<'_> {
    type Item = Contents;

    fn next(&mut self) -> Option<Self::Item> {
        if self.results.len() < 1 {
            if self.complete {
                return None;
            }

            match self
                .client
                ._list_objects(&self.bucket, &self.prefix, &self.start_after, &self.continuation_token)
            {
                Ok(mut v) => {
                    for o in v.contents.drain(..) {
                        self.results.push_back(o);
                    }
                    if v.next_token.is_some() {
                        self.continuation_token = v.next_token;
                    } else {
                        self.complete = true;
                    }
                }
                Err(e) => {
                    error!(e);
                    return None;
                }
            }
        }

        Some(self.results.pop_front().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::se::to_string;

    #[test]
    fn test_bucket_list_response() {
        let res = ListAllMyBucketsResult {
            owner: Owner {
                id: "asdfasdfa".to_string(),
                display_name: "12315123".to_string(),
            },
            buckets: Buckets {
                list: vec![
                    Bucket {
                        name: "asdasdfasdfadfadf".to_string(),
                        creation_date: "1238218238902389023890".to_string(),
                    },
                    Bucket {
                        name: "asdasdfasdfadfadf".to_string(),
                        creation_date: "1238218238902389023890".to_string(),
                    },
                    Bucket {
                        name: "asdasdfasdfadfadf".to_string(),
                        creation_date: "1238218238902389023890".to_string(),
                    },
                ],
            },
        };

        let exp = "<ListAllMyBucketsResult><Owner><ID>asdfasdfa</ID><DisplayName>12315123</DisplayName></Owner><Buckets><Bucket><Name>asdasdfasdfadfadf</Name><CreationDate>1238218238902389023890</CreationDate></Bucket><Bucket><Name>asdasdfasdfadfadf</Name><CreationDate>1238218238902389023890</CreationDate></Bucket><Bucket><Name>asdasdfasdfadfadf</Name><CreationDate>1238218238902389023890</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>";

        let out = to_string(&res).unwrap();
        assert_eq!(out, exp);
    }
}
