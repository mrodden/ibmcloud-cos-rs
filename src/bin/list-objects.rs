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
use std::time::{Duration, Instant};

use ibmcloud_iam::token::{TokenManager, DEFAULT_IAM_ENDPOINT};
use tracing_subscriber;

use ibmcloud_cos::cos;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut bucket = "".to_string();
    let mut endpoint = "".to_string();
    let mut prefix = None;

    for (i, arg) in std::env::args().enumerate() {
        match i {
            1 => endpoint = arg,
            2 => bucket = arg,
            3 => prefix = Some(arg),
            _ => {}
        }
    }

    eprintln!("Listing {}", bucket);

    let tm = Arc::new(TokenManager::default());
    let c = cos::Client::new(tm, &endpoint);

    for obj in c.list_objects(&bucket, prefix) {
        println!("{} {:>10} {}", obj.last_modified, obj.size, obj.key);
    }

    Ok(())
}
