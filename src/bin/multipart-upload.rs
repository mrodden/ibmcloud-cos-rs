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

use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use ibmcloud_iam::token::TokenManager;
use tracing_subscriber;

use ibmcloud_cos::cos;
use ibmcloud_cos::multipartupload::{CompleteMultipartUpload, Part};

const MB: usize = 1 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    endpoint: String,
    bucket: String,
    key: String,
    filename: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let tm = Arc::new(TokenManager::default());

    let c = cos::Client::new(tm, &args.endpoint);

    let mut file = File::open(args.filename)?;
    let mut parts: Vec<Part> = Vec::new();

    let upload_id = c.create_multipart_upload(&args.bucket, &args.key)?;

    loop {
        let mut chunk = vec![0u8; 5 * MB];

        let n = file.read(&mut chunk[..])?;

        if n == 0 {
            break;
        }

        chunk.truncate(n);

        let seq_no = parts.len() + 1;

        let part = c.upload_part(&args.bucket, &args.key, &upload_id, seq_no, chunk)?;
        parts.push(part);
    }

    let cmu = CompleteMultipartUpload { parts };

    if let Err(_) = c.complete_multipart_upload(&args.bucket, &args.key, &upload_id, cmu) {
        let _ = c.abort_multipart_upload(&args.bucket, &args.key, &upload_id)?;
    }

    Ok(())
}
