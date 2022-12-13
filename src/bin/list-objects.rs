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

use std::sync::Arc;

use clap::Parser;
use ibmcloud_iam::token::TokenManager;
use tracing_subscriber;

use ibmcloud_cos::cos;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    endpoint: String,
    bucket: String,
    prefix: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    eprintln!("Listing {}", args.bucket);

    let tm = Arc::new(TokenManager::default());
    let c = cos::Client::new(tm, &args.endpoint);

    for obj in c.list_objects(&args.bucket, args.prefix, None) {
        println!("{} {:>10} {}", obj.last_modified, obj.size, obj.key);
    }

    Ok(())
}
