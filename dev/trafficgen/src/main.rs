//! Publishes fake, repeatable-ish MQTT traffic against the dev Broker so mÅqtt has something
//! realistic to explore: slowly drifting JSON sensors, retained status topics, bursts, binary
//! payloads, and optionally a wide fleet of topics for stress-testing the Topic Tree.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::json;
use tokio::time::{MissedTickBehavior, interval, sleep};

const TICK: Duration = Duration::from_millis(10);
const ROOMS: &[&str] = &[
    "kitchen",
    "living-room",
    "bedroom",
    "office",
    "garage",
    "hallway",
];
const KINDS: &[&str] = &["climate", "power", "motion"];
/// Battery percentage points each device loses per second, on average.
const BATTERY_DRAIN_PER_SEC: f64 = 1.0 / 180.0;

#[derive(Parser, Debug, Clone)]
#[command(
    about = "Generate fake MQTT traffic for mÅqtt development",
    args_override_self = true
)]
struct Args {
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value_t = 1883)]
    port: u16,
    #[arg(long)]
    username: Option<String>,
    #[arg(long, requires = "username")]
    password: Option<String>,
    /// Root of every published topic.
    #[arg(long, default_value = "maqtt")]
    prefix: String,
    /// Number of simulated sensor devices.
    #[arg(long, default_value_t = 24)]
    devices: usize,
    /// Sensor messages per second, spread across all devices.
    #[arg(long, default_value_t = 50.0)]
    rate: f64,
    /// Number of extra `fleet/<id>/telemetry` topics (use thousands to stress the Topic Tree).
    #[arg(long, default_value_t = 0)]
    fleet: usize,
    /// Fleet messages per second, spread across all fleet topics.
    #[arg(long, default_value_t = 200.0)]
    fleet_rate: f64,
    /// Seconds between bursts of rapid-fire events; 0 disables bursts.
    #[arg(long, default_value_t = 10)]
    burst_every: u64,
    /// Messages per burst.
    #[arg(long, default_value_t = 200)]
    burst_size: usize,
    /// Seed for the random generator, for reproducible payloads.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = connect(&args);
    let sent = Arc::new(AtomicU64::new(0));

    publish_statuses(&client, &args).await?;

    tokio::spawn(sensors(client.clone(), args.clone(), sent.clone()));
    tokio::spawn(status_flips(client.clone(), args.clone(), sent.clone()));
    tokio::spawn(raw_binary(client.clone(), args.clone(), sent.clone()));
    tokio::spawn(mqtt5_properties(
        connect_v5(&args),
        args.clone(),
        sent.clone(),
    ));
    if args.burst_every > 0 {
        tokio::spawn(bursts(client.clone(), args.clone(), sent.clone()));
    }
    if args.fleet > 0 {
        tokio::spawn(fleet(client.clone(), args.clone(), sent.clone()));
    }
    tokio::spawn(report(sent));

    tokio::signal::ctrl_c().await?;
    eprintln!("trafficgen: stopping");
    Ok(())
}

fn connect(args: &Args) -> AsyncClient {
    let mut options = MqttOptions::new(
        format!("maqtt-trafficgen-{}", std::process::id()),
        &args.host,
        args.port,
    );
    options.set_keep_alive(Duration::from_secs(10));
    if let (Some(user), Some(pass)) = (&args.username, &args.password) {
        options.set_credentials(user, pass);
    }
    let (client, mut eventloop) = AsyncClient::new(options, 1024);

    let target = format!("{}:{}", args.host, args.port);
    tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop.poll().await {
                Ok(_) if !connected => {
                    connected = true;
                    eprintln!("trafficgen: connected to {target}");
                }
                Ok(_) => {}
                Err(err) => {
                    connected = false;
                    eprintln!("trafficgen: connection error: {err}, retrying in 2s");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    });
    client
}

/// A second, MQTT 5 client, for messages that carry properties.
fn connect_v5(args: &Args) -> rumqttc::v5::AsyncClient {
    let mut options = rumqttc::v5::MqttOptions::new(
        format!("maqtt-trafficgen-v5-{}", std::process::id()),
        &args.host,
        args.port,
    );
    options.set_keep_alive(Duration::from_secs(10));
    if let (Some(user), Some(pass)) = (&args.username, &args.password) {
        options.set_credentials(user, pass);
    }
    let (client, mut eventloop) = rumqttc::v5::AsyncClient::new(options, 64);
    tokio::spawn(async move {
        loop {
            if let Err(err) = eventloop.poll().await {
                eprintln!("trafficgen: MQTT 5 connection error: {err}, retrying in 2s");
                sleep(Duration::from_secs(2)).await;
            }
        }
    });
    client
}

fn device_names(args: &Args) -> Vec<String> {
    (0..args.devices)
        .map(|i| {
            let room = ROOMS[i % ROOMS.len()];
            let kind = KINDS[(i / ROOMS.len()) % KINDS.len()];
            format!("{room}/{kind}-{}", i / (ROOMS.len() * KINDS.len()) + 1)
        })
        .collect()
}

/// Turns a messages-per-second rate into a per-tick count, carrying the fractional remainder.
struct Pacer {
    per_tick: f64,
    carry: f64,
}

impl Pacer {
    fn new(rate: f64) -> Self {
        Self {
            per_tick: rate * TICK.as_secs_f64(),
            carry: 0.0,
        }
    }

    fn next(&mut self) -> usize {
        self.carry += self.per_tick;
        let n = self.carry.floor();
        self.carry -= n;
        n as usize
    }
}

fn ticker() -> tokio::time::Interval {
    let mut tick = interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick
}

async fn publish_statuses(client: &AsyncClient, args: &Args) -> Result<()> {
    for device in device_names(args) {
        let topic = format!("{}/status/{device}", args.prefix);
        client
            .publish(topic, QoS::AtLeastOnce, true, "online")
            .await?;
    }
    Ok(())
}

/// Every few seconds a random device goes offline or comes back, as a retained status.
async fn status_flips(client: AsyncClient, args: Args, sent: Arc<AtomicU64>) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(args.seed ^ 0x5747);
    let devices = device_names(&args);
    let mut online = vec![true; devices.len()];
    loop {
        sleep(Duration::from_secs(rng.random_range(3..8))).await;
        let i = rng.random_range(0..devices.len());
        online[i] = !online[i];
        let topic = format!("{}/status/{}", args.prefix, devices[i]);
        let payload = if online[i] { "online" } else { "offline" };
        client
            .publish(topic, QoS::AtLeastOnce, true, payload)
            .await?;
        sent.fetch_add(1, Ordering::Relaxed);
    }
}

/// Full-state JSON readings that drift slowly, so consecutive messages differ in a few fields.
async fn sensors(client: AsyncClient, args: Args, sent: Arc<AtomicU64>) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(args.seed);
    let devices = device_names(&args);
    let mut temps: Vec<f64> = devices
        .iter()
        .map(|_| rng.random_range(18.0..24.0))
        .collect();
    let mut batteries: Vec<u8> = devices.iter().map(|_| rng.random_range(60..=100)).collect();
    // Drain about 1% battery per device every few minutes, independent of the message rate.
    let per_device_rate = args.rate / devices.len().max(1) as f64;
    let drain_chance = (BATTERY_DRAIN_PER_SEC / per_device_rate).min(1.0);
    let mut seq = 0u64;
    let mut next = 0usize;
    let mut pacer = Pacer::new(args.rate);
    let mut tick = ticker();
    loop {
        tick.tick().await;
        for _ in 0..pacer.next() {
            let i = next;
            next = (next + 1) % devices.len();
            seq += 1;
            temps[i] = (temps[i] + rng.random_range(-0.2..0.2)).clamp(10.0, 35.0);
            if rng.random_bool(drain_chance) {
                batteries[i] = batteries[i].saturating_sub(1);
            }
            let payload = json!({
                "seq": seq,
                "temperature": (temps[i] * 10.0).round() / 10.0,
                "humidity": rng.random_range(35..60),
                "battery": batteries[i],
                "firmware": "1.4.2",
            });
            let topic = format!("{}/home/{}", args.prefix, devices[i]);
            client
                .publish(topic, QoS::AtMostOnce, false, payload.to_string())
                .await?;
            sent.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Short bursts of plain-text events, to exercise batching and the Message Log.
async fn bursts(client: AsyncClient, args: Args, sent: Arc<AtomicU64>) -> Result<()> {
    let topic = format!("{}/burst/events", args.prefix);
    let mut burst = 0u64;
    loop {
        sleep(Duration::from_secs(args.burst_every)).await;
        burst += 1;
        for n in 0..args.burst_size {
            let payload = format!("burst {burst} event {n}");
            client
                .publish(&topic, QoS::AtMostOnce, false, payload)
                .await?;
        }
        sent.fetch_add(args.burst_size as u64, Ordering::Relaxed);
    }
}

/// Random binary payloads that are not valid UTF-8, to exercise the hex view.
async fn raw_binary(client: AsyncClient, args: Args, sent: Arc<AtomicU64>) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(args.seed ^ 0xb1a2);
    loop {
        sleep(Duration::from_secs(1)).await;
        let len = rng.random_range(16..64);
        let mut payload = vec![0u8; len];
        rng.fill(&mut payload[..]);
        payload[0] = 0xff; // never valid UTF-8
        let topic = format!("{}/raw/modem-{}", args.prefix, rng.random_range(1..=3));
        client
            .publish(topic, QoS::AtMostOnce, false, payload)
            .await?;
        sent.fetch_add(1, Ordering::Relaxed);
    }
}

/// A thermostat speaking MQTT 5, with every kind of publish property set.
async fn mqtt5_properties(
    client: rumqttc::v5::AsyncClient,
    args: Args,
    sent: Arc<AtomicU64>,
) -> Result<()> {
    use rumqttc::v5::mqttbytes::QoS as QoS5;
    use rumqttc::v5::mqttbytes::v5::PublishProperties;

    let mut rng = StdRng::seed_from_u64(args.seed ^ 0x5555);
    let topic = format!("{}/v5/thermostat", args.prefix);
    let mut request = 0u64;
    let mut setpoint = 21.0f64;
    loop {
        sleep(Duration::from_secs(2)).await;
        request += 1;
        if rng.random_bool(0.2) {
            setpoint += if rng.random_bool(0.5) { 0.5 } else { -0.5 };
        }
        let payload = json!({
            "setpoint": setpoint,
            "mode": if setpoint > 21.0 { "heat" } else { "eco" },
            "request": request,
        });
        let properties = PublishProperties {
            payload_format_indicator: Some(1),
            message_expiry_interval: Some(60),
            content_type: Some("application/json".into()),
            response_topic: Some(format!("{topic}/reply")),
            correlation_data: Some(format!("req-{request}").into_bytes().into()),
            user_properties: vec![
                ("source".into(), "trafficgen".into()),
                ("unit".into(), "celsius".into()),
            ],
            ..PublishProperties::default()
        };
        client
            .publish_with_properties(
                &topic,
                QoS5::AtLeastOnce,
                false,
                payload.to_string(),
                properties,
            )
            .await?;
        sent.fetch_add(1, Ordering::Relaxed);
    }
}

/// Many distinct topics with small payloads, for stress-testing the Topic Tree.
async fn fleet(client: AsyncClient, args: Args, sent: Arc<AtomicU64>) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(args.seed ^ 0xf1ee7);
    let mut next = 0usize;
    let mut pacer = Pacer::new(args.fleet_rate);
    let mut tick = ticker();
    loop {
        tick.tick().await;
        for _ in 0..pacer.next() {
            let id = next;
            next = (next + 1) % args.fleet;
            let topic = format!("{}/fleet/{:05}/telemetry", args.prefix, id);
            let payload = json!({ "rssi": rng.random_range(-90..-40), "uptime": rng.random_range(0..86_400) });
            client
                .publish(topic, QoS::AtMostOnce, false, payload.to_string())
                .await?;
            sent.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn report(sent: Arc<AtomicU64>) {
    let mut last = 0;
    loop {
        sleep(Duration::from_secs(5)).await;
        let total = sent.load(Ordering::Relaxed);
        eprintln!("trafficgen: {} msg/s, {total} total", (total - last) / 5);
        last = total;
    }
}
