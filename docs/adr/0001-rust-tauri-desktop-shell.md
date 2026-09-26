# Rust + Tauri as the application stack

måqtt is a Rust backend (MQTT via `rumqttc`) with a web frontend rendered by Tauri, targeting Linux and macOS. We chose Tauri over pure-Rust GUI toolkits (egui, Iced) because the explorer needs rich widgets — virtualized trees and logs, JSON viewers — and planned data visualization benefits from the JS charting ecosystem, which is far ahead of Rust's. The cost is two languages and an IPC boundary between them.
