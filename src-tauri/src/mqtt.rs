//! One face over `rumqttc`'s separate MQTT 3.1.1 and MQTT 5 clients, so the rest of måqtt
//! never needs to know which protocol a Connection speaks.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rumqttc::{Outgoing, TlsConfiguration, Transport};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};

use crate::profile::{Profile, Protocol, TlsSettings};

/// The largest packet MQTT allows. `rumqttc` defaults to 10 KiB and drops the Connection on
/// anything bigger, which is wrong for a tool meant to show whatever the Broker sends.
const MAX_PACKET_SIZE: u32 = 268_435_455;
/// How long a persistent session outlives a disconnect on an MQTT 5 Broker.
const PERSISTENT_SESSION_EXPIRY_SECS: u32 = 7 * 24 * 60 * 60;
const REQUEST_CAPACITY: usize = 64;

#[derive(Debug)]
pub enum Incoming {
    ConnAck,
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        retain: bool,
    },
    /// A Subscribe request left for the Broker with this packet id, in request order.
    SubscribeSent {
        pkid: u16,
    },
    /// The Broker's answer per filter: the granted QoS, or why it refused.
    SubAck {
        pkid: u16,
        results: Vec<Result<u8, String>>,
    },
    DisconnectSent,
    Other,
}

#[derive(Clone)]
pub enum Client {
    V311(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

pub enum EventLoop {
    // Boxed: both are large, and there is only one per Connection.
    V311(Box<rumqttc::EventLoop>),
    V5(Box<rumqttc::v5::EventLoop>),
}

pub fn connect(profile: &Profile, password: Option<&str>) -> Result<(Client, EventLoop), String> {
    let transport = match &profile.tls {
        Some(tls) => Transport::tls_with_config(TlsConfiguration::Rustls(tls_config(tls)?)),
        None => Transport::tcp(),
    };
    let keep_alive = Duration::from_secs(profile.keep_alive_secs.max(5).into());
    let credentials = profile
        .username
        .as_deref()
        .filter(|u| !u.is_empty())
        .map(|u| (u, password.unwrap_or_default()));

    Ok(match profile.protocol {
        Protocol::Mqtt311 => {
            let mut options =
                rumqttc::MqttOptions::new(&profile.client_id, &profile.host, profile.port);
            options
                .set_transport(transport)
                .set_keep_alive(keep_alive)
                .set_clean_session(profile.clean_start)
                .set_max_packet_size(MAX_PACKET_SIZE as usize, MAX_PACKET_SIZE as usize);
            if let Some((user, pass)) = credentials {
                options.set_credentials(user, pass);
            }
            let (client, eventloop) = rumqttc::AsyncClient::new(options, REQUEST_CAPACITY);
            (Client::V311(client), EventLoop::V311(Box::new(eventloop)))
        }
        Protocol::Mqtt5 => {
            let mut options =
                rumqttc::v5::MqttOptions::new(&profile.client_id, &profile.host, profile.port);
            options
                .set_transport(transport)
                .set_keep_alive(keep_alive)
                .set_clean_start(profile.clean_start)
                .set_max_packet_size(Some(MAX_PACKET_SIZE));
            if !profile.clean_start {
                options.set_session_expiry_interval(Some(PERSISTENT_SESSION_EXPIRY_SECS));
            }
            if let Some((user, pass)) = credentials {
                options.set_credentials(user, pass);
            }
            let (client, eventloop) = rumqttc::v5::AsyncClient::new(options, REQUEST_CAPACITY);
            (Client::V5(client), EventLoop::V5(Box::new(eventloop)))
        }
    })
}

impl Client {
    pub fn try_subscribe(&self, filter: &str, qos: u8) -> Result<(), String> {
        match self {
            Client::V311(c) => c
                .try_subscribe(filter, v311_qos(qos))
                .map_err(|err| err.to_string()),
            Client::V5(c) => c
                .try_subscribe(filter, v5_qos(qos))
                .map_err(|err| err.to_string()),
        }
    }

    pub fn try_unsubscribe(&self, filter: &str) -> Result<(), String> {
        match self {
            Client::V311(c) => c.try_unsubscribe(filter).map_err(|err| err.to_string()),
            Client::V5(c) => c.try_unsubscribe(filter).map_err(|err| err.to_string()),
        }
    }

    pub fn try_disconnect(&self) -> Result<(), String> {
        match self {
            Client::V311(c) => c.try_disconnect().map_err(|err| err.to_string()),
            Client::V5(c) => c.try_disconnect().map_err(|err| err.to_string()),
        }
    }
}

impl EventLoop {
    pub async fn poll(&mut self) -> Result<Incoming, String> {
        match self {
            EventLoop::V311(eventloop) => {
                use rumqttc::{Event, Packet, SubscribeReasonCode};
                Ok(match eventloop.poll().await.map_err(|e| e.to_string())? {
                    Event::Incoming(Packet::ConnAck(_)) => Incoming::ConnAck,
                    Event::Incoming(Packet::Publish(p)) => Incoming::Publish {
                        topic: p.topic,
                        payload: p.payload,
                        qos: p.qos as u8,
                        retain: p.retain,
                    },
                    Event::Incoming(Packet::SubAck(ack)) => Incoming::SubAck {
                        pkid: ack.pkid,
                        results: ack
                            .return_codes
                            .into_iter()
                            .map(|code| match code {
                                SubscribeReasonCode::Success(qos) => Ok(qos as u8),
                                SubscribeReasonCode::Failure => Err("Refused by the Broker".into()),
                            })
                            .collect(),
                    },
                    Event::Outgoing(outgoing) => outgoing_event(outgoing),
                    Event::Incoming(_) => Incoming::Other,
                })
            }
            EventLoop::V5(eventloop) => {
                use rumqttc::v5::Event;
                use rumqttc::v5::mqttbytes::v5::{Packet, SubscribeReasonCode};
                Ok(match eventloop.poll().await.map_err(|e| e.to_string())? {
                    Event::Incoming(Packet::ConnAck(_)) => Incoming::ConnAck,
                    Event::Incoming(Packet::Publish(p)) => Incoming::Publish {
                        topic: String::from_utf8_lossy(&p.topic).into_owned(),
                        payload: p.payload,
                        qos: p.qos as u8,
                        retain: p.retain,
                    },
                    Event::Incoming(Packet::SubAck(ack)) => Incoming::SubAck {
                        pkid: ack.pkid,
                        results: ack
                            .return_codes
                            .into_iter()
                            .map(|code| match code {
                                SubscribeReasonCode::Success(qos) => Ok(qos as u8),
                                other => Err(format!("{other:?}")),
                            })
                            .collect(),
                    },
                    Event::Outgoing(outgoing) => outgoing_event(outgoing),
                    Event::Incoming(_) => Incoming::Other,
                })
            }
        }
    }
}

fn outgoing_event(outgoing: Outgoing) -> Incoming {
    match outgoing {
        Outgoing::Subscribe(pkid) => Incoming::SubscribeSent { pkid },
        Outgoing::Disconnect => Incoming::DisconnectSent,
        _ => Incoming::Other,
    }
}

fn v311_qos(qos: u8) -> rumqttc::QoS {
    match qos {
        0 => rumqttc::QoS::AtMostOnce,
        1 => rumqttc::QoS::AtLeastOnce,
        _ => rumqttc::QoS::ExactlyOnce,
    }
}

fn v5_qos(qos: u8) -> rumqttc::v5::mqttbytes::QoS {
    use rumqttc::v5::mqttbytes::QoS;
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

pub fn tls_config(tls: &TlsSettings) -> Result<Arc<ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|err| err.to_string())?;

    let builder = if tls.insecure_skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate(provider)))
    } else {
        builder.with_root_certificates(root_certificates(tls)?)
    };

    let config = match (&tls.client_cert_file, &tls.client_key_file) {
        (Some(cert_file), Some(key_file)) => {
            let certs = CertificateDer::pem_file_iter(cert_file)
                .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
                .map_err(|err| format!("Couldn't read {}: {err}", cert_file.display()))?;
            let key = PrivateKeyDer::from_pem_file(key_file)
                .map_err(|err| format!("Couldn't read {}: {err}", key_file.display()))?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|err| format!("Client certificate rejected: {err}"))?
        }
        _ => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

fn root_certificates(tls: &TlsSettings) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    match &tls.ca_file {
        Some(ca_file) => {
            for cert in CertificateDer::pem_file_iter(ca_file)
                .map_err(|err| format!("Couldn't read {}: {err}", ca_file.display()))?
            {
                let cert = cert.map_err(|err| format!("Bad CA in {}: {err}", ca_file.display()))?;
                roots
                    .add(cert)
                    .map_err(|err| format!("Bad CA in {}: {err}", ca_file.display()))?;
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            roots.add_parsable_certificates(native.certs);
        }
    }
    if roots.is_empty() {
        return Err("No trusted CA certificates found".into());
    }
    Ok(roots)
}

/// For the "skip verification" option: trusts any certificate, but still checks that the
/// handshake is signed by the certificate presented.
#[derive(Debug)]
struct AcceptAnyCertificate(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Against the dev Broker from `dev/env.sh`; run with `cargo test -p maqtt -- --ignored`.
#[cfg(test)]
mod dev_broker_tests {
    use super::*;
    use std::path::PathBuf;

    fn certs() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../dev/broker/generated")
    }

    fn profile(port: u16, protocol: Protocol) -> Profile {
        let mut profile = Profile::new("test", "localhost", port);
        profile.protocol = protocol;
        profile
    }

    /// Connects, subscribes to `#`, and returns the granted QoS or the first error.
    async fn connect_and_subscribe(
        profile: &Profile,
        password: Option<&str>,
    ) -> Result<u8, String> {
        let (client, mut eventloop) = connect(profile, password)?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match eventloop.poll().await? {
                    Incoming::ConnAck => client.try_subscribe("maqtt/#", 1)?,
                    Incoming::SubAck { mut results, .. } => return results.remove(0),
                    _ => {}
                }
            }
        })
        .await
        .map_err(|_| "timed out".to_string())?
    }

    fn with_tls(mut profile: Profile, client_cert: bool) -> Profile {
        profile.tls = Some(TlsSettings {
            ca_file: Some(certs().join("ca.crt")),
            client_cert_file: client_cert.then(|| certs().join("client.crt")),
            client_key_file: client_cert.then(|| certs().join("client.key")),
            insecure_skip_verify: false,
        });
        profile
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn every_listener_accepts_both_protocols() {
        for protocol in [Protocol::Mqtt5, Protocol::Mqtt311] {
            let plain = profile(21883, protocol);
            assert_eq!(
                connect_and_subscribe(&plain, None).await,
                Ok(1),
                "{protocol:?} plain"
            );

            let mut auth = profile(21884, protocol);
            auth.username = Some("maqtt".into());
            assert_eq!(
                connect_and_subscribe(&auth, Some("maqtt")).await,
                Ok(1),
                "{protocol:?} auth"
            );

            let mut tls = with_tls(profile(28883, protocol), false);
            tls.username = Some("maqtt".into());
            assert_eq!(
                connect_and_subscribe(&tls, Some("maqtt")).await,
                Ok(1),
                "{protocol:?} tls"
            );

            let mtls = with_tls(profile(28884, protocol), true);
            assert_eq!(
                connect_and_subscribe(&mtls, None).await,
                Ok(1),
                "{protocol:?} mtls"
            );
        }
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn a_wrong_password_is_refused() {
        let mut auth = profile(21884, Protocol::Mqtt5);
        auth.username = Some("maqtt".into());
        assert!(connect_and_subscribe(&auth, Some("wrong")).await.is_err());
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn tls_without_the_ca_fails_unless_verification_is_skipped() {
        let mut tls = profile(28883, Protocol::Mqtt5);
        tls.username = Some("maqtt".into());
        tls.tls = Some(TlsSettings::default());
        assert!(connect_and_subscribe(&tls, Some("maqtt")).await.is_err());

        tls.tls = Some(TlsSettings {
            insecure_skip_verify: true,
            ..TlsSettings::default()
        });
        assert_eq!(connect_and_subscribe(&tls, Some("maqtt")).await, Ok(1));
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn mtls_without_a_client_certificate_is_refused() {
        let mtls = with_tls(profile(28884, Protocol::Mqtt5), false);
        assert!(connect_and_subscribe(&mtls, None).await.is_err());
    }
}
