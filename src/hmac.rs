use std::collections::BTreeMap;
use std::fmt::Write;
use std::io::Read;

use chrono::{DateTime, Utc};
use hex;
use hmac::{Hmac, Mac};
use reqwest;
use sha2::{Digest, Sha256};
use tracing::{debug, trace};
use urlencoding::encode;

use crate::cos::{check_response, Error};

const SIGTYPENAME: &str = "AWS4-HMAC-SHA256";

fn canonicalize_uri(path: &str) -> String {
    path.to_string()
}

fn canonicalize_query_params(params: BTreeMap<String, String>) -> Result<String, Error> {
    let mut pairs = vec![];

    for (key, value) in params.iter() {
        pairs.push(format!("{}={}", encode(key), encode(value)));
    }

    Ok(pairs.join("&"))
}

fn canonicalize_headers(headers: BTreeMap<String, String>) -> Result<(String, String), Error> {
    let mut cheaders = String::new();
    let mut header_list = vec![];

    for (key, value) in headers.iter() {
        writeln!(cheaders, "{}:{}", key.to_lowercase(), value)?;
        header_list.push(key.to_string());
    }

    let header_list_str = header_list.join(";");

    Ok((cheaders, header_list_str))
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("error with key size");
    mac.update(data);
    let res = mac.finalize();

    res.into_bytes().to_vec()
}

fn hexdigest(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub fn sign(
    access_key_id: &str,
    secret_access_key: &str,
    date: DateTime<Utc>,
    http_method: &str,
    path: &str,
    query_params: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
    payload_hash: &str,
) -> Result<String, Error> {
    let region = "us-standard";

    let mut creq = String::new();

    writeln!(creq, "{}", http_method)?;

    writeln!(creq, "{}", canonicalize_uri(path))?;

    writeln!(creq, "{}", canonicalize_query_params(query_params)?)?;

    let (cheaders, signed_headers) = canonicalize_headers(headers)?;
    writeln!(creq, "{}", cheaders)?;
    writeln!(creq, "{}", signed_headers)?;

    write!(creq, "{}", payload_hash)?;
    trace!("CanonicalRequest: {:?}", creq);
    trace!("CanonicalRequestBytes: {:?}", creq.as_bytes());

    let hashed_creq = hexdigest(creq.as_bytes());

    let mut string_to_sign = String::new();

    let timestamp = format!("{}", date.format("%Y%m%dT%H%M%SZ"));
    let datestamp = format!("{}", date.format("%Y%m%d"));
    let scope = format!("{}/{}/s3/aws4_request", datestamp, region);

    writeln!(string_to_sign, "{}", SIGTYPENAME)?;
    writeln!(string_to_sign, "{}", timestamp)?;
    writeln!(string_to_sign, "{}", scope)?;
    write!(string_to_sign, "{}", hashed_creq)?;

    trace!("StringToSign: {:?}", string_to_sign);
    trace!("StringToSignBytes: {:?}", string_to_sign.as_bytes());

    let datekey = hmac(
        &format!("AWS4{}", secret_access_key).as_bytes(),
        datestamp.as_bytes(),
    );
    let dateregionkey = hmac(&datekey, region.as_bytes());
    let dateregionservicekey = hmac(&dateregionkey, b"s3");
    let signing_key = hmac(&dateregionservicekey, b"aws4_request");

    let sig_bytes = hmac(&signing_key, string_to_sign.as_bytes());
    let sig = hex::encode(sig_bytes);

    let mut header = String::new();
    write!(header, "{} ", SIGTYPENAME)?;
    write!(header, "Credential={}/{},", access_key_id, scope)?;
    write!(header, "SignedHeaders={},", signed_headers)?;
    write!(header, "Signature={}", sig)?;

    Ok(header)
}

pub struct Client {
    access_key_id: String,
    secret_access_key: String,

    pub(crate) endpoint: String,
    pub(crate) client: reqwest::blocking::Client,
}

impl Client {
    pub fn new(endpoint: &str, access_key_id: &str, secret_access_key: &str) -> Self {
        Self {
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            endpoint: endpoint.to_string(),
            client: reqwest::blocking::Client::new(),
        }
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Box<dyn Read>, Error> {
        let c = &self.client;
        let url = format!("https://{}/{}/{}", self.endpoint, bucket, key);

        let mut headers = BTreeMap::new();
        headers.insert("host".to_string(), self.endpoint.clone());

        let now = Utc::now();
        let timestamp = format!("{}", now.format("%Y%m%dT%H%M%SZ"));
        headers.insert("x-amz-date".to_string(), timestamp.clone());

        let params = BTreeMap::new();

        let sig = sign(
            &self.access_key_id,
            &self.secret_access_key,
            now,
            "GET",
            &format!("/{}/{}", bucket, key),
            params,
            headers,
            &hexdigest(b""),
        )?;

        trace!("Sig: {:?}", sig);

        let req = c
            .get(url)
            .header("Authorization", sig)
            .header("x-amz-date", timestamp);

        debug!("{:?}", req);

        let response = req.send()?;

        let r = check_response(response)?;
        Ok(Box::new(r))
    }

    pub fn put_object<B: Into<reqwest::blocking::Body>>(
        &self,
        bucket: &str,
        key: &str,
        body: B,
    ) -> Result<(), Error> {
        let c = &self.client;
        let url = format!("https://{}/{}/{}", self.endpoint, bucket, key);

        let mut headers = BTreeMap::new();
        headers.insert("host".to_string(), self.endpoint.clone());

        let now = Utc::now();
        let timestamp = format!("{}", now.format("%Y%m%dT%H%M%SZ"));
        headers.insert("x-amz-date".to_string(), timestamp.clone());
        headers.insert(
            "x-amz-content-sha256".to_string(),
            "UNSIGNED-PAYLOAD".to_string(),
        );

        let params = BTreeMap::new();

        let sig = sign(
            &self.access_key_id,
            &self.secret_access_key,
            now,
            "PUT",
            &format!("/{}/{}", bucket, key),
            params,
            headers,
            "UNSIGNED-PAYLOAD",
        )?;

        trace!("Sig: {:?}", sig);

        let response = c
            .put(url)
            .header("Authorization", sig)
            .header("x-amz-date", timestamp)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .body(body)
            .send()?;

        let _r = check_response(response)?;
        Ok(())
    }
}
