//! Debug builds only: on first run, Broker Profiles for the dev Broker from `dev/env.sh`, so a
//! fresh checkout can connect straight away.

use std::path::PathBuf;

use crate::profile::{Profile, TlsSettings};
use crate::profile_store::{PasswordChange, ProfileStore};

pub fn seed(store: &mut ProfileStore) -> Result<(), String> {
    let certs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../dev/broker/generated");
    let certs = certs.canonicalize().unwrap_or(certs);

    let plain = Profile::new("Dev Broker", "localhost", 21883);

    let mut tls = Profile::new("Dev Broker (TLS)", "localhost", 28883);
    tls.username = Some("maqtt".into());
    tls.tls = Some(TlsSettings {
        ca_file: Some(certs.join("ca.crt")),
        ..TlsSettings::default()
    });

    let mut mtls = Profile::new("Dev Broker (mTLS)", "localhost", 28884);
    mtls.tls = Some(TlsSettings {
        ca_file: Some(certs.join("ca.crt")),
        client_cert_file: Some(certs.join("client.crt")),
        client_key_file: Some(certs.join("client.key")),
        insecure_skip_verify: false,
    });

    store.save(plain, PasswordChange::Keep)?;
    store.save(tls, PasswordChange::Set("maqtt".into()))?;
    store.save(mtls, PasswordChange::Keep)
}
