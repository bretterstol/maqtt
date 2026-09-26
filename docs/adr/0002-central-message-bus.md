# Messages flow through a central backend bus, not directly to the UI

Every Connection publishes incoming messages onto a single bus in the Rust backend; the UI, history buffers, and (later) processing all consume from that bus. We chose this over wiring each Connection straight to the frontend because planned in-flight processing may transform messages from one Connection and republish them to another, which needs a Connection-agnostic place to route through.
