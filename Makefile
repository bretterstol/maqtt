# Shortcuts for developing and running måqtt. `make` on its own lists them.

.DEFAULT_GOAL := help
SHELL := bash

# Whatever version was built last.
APPIMAGE = $(lastword $(sort $(wildcard target/release/bundle/appimage/maqtt_*_amd64.AppImage)))
RPM = $(lastword $(sort $(wildcard target/release/bundle/rpm/maqtt-*.x86_64.rpm)))
SANDBOX := /tmp/maqtt-sandbox

.PHONY: help dev sandbox app broker traffic stress stop test test-broker lint format build run-release install-rpm clean-sandbox

help: ## List the shortcuts
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'

node_modules: package.json pnpm-lock.yaml
	pnpm install
	@touch node_modules

dev: node_modules ## Dev Broker, fake traffic and the app, all in one terminal
	dev/env.sh broker
	@cargo build -q -p trafficgen
	@target/debug/trafficgen --port 21883 > /tmp/maqtt-trafficgen.log 2>&1 & \
	  trap "kill $$! 2>/dev/null" EXIT; \
	  echo "trafficgen running (log: /tmp/maqtt-trafficgen.log)"; \
	  pnpm tauri dev

sandbox: node_modules ## Like dev, but with a throwaway config in /tmp/maqtt-sandbox, all connected
	dev/env.sh broker
	@cargo build -q -p trafficgen
	@target/debug/trafficgen --port 21883 > /tmp/maqtt-trafficgen.log 2>&1 & \
	  trap "kill $$! 2>/dev/null" EXIT; \
	  MAQTT_CONFIG_DIR=$(SANDBOX) MAQTT_AUTOCONNECT=all pnpm tauri dev

app: node_modules ## Just the app (connect to your own Brokers)
	pnpm tauri dev

broker: ## Start the dev Broker in the background
	dev/env.sh broker

traffic: ## Dev Broker plus fake traffic in this terminal (Ctrl+C stops the traffic)
	dev/env.sh up

stress: ## Dev Broker plus heavy traffic: 10,000 extra topics
	dev/env.sh up --fleet 10000

stop: ## Stop the dev Broker
	dev/env.sh down

test: node_modules ## Unit tests (Vitest and cargo test)
	pnpm test

test-broker: broker ## Integration tests against the dev Broker (restarts it)
	pnpm test:broker

lint: node_modules ## Type checks, ESLint, Prettier, rustfmt, clippy
	pnpm lint

format: node_modules ## Format everything
	pnpm format

build: node_modules ## Release build: .deb, .rpm and AppImage in target/release/bundle
	pnpm tauri build

run-release: ## Run the release AppImage (after make build)
	@test -n "$(APPIMAGE)" || { echo "No AppImage yet; run make build first."; exit 1; }
	$(APPIMAGE)

install-rpm: ## Install the release .rpm on this machine (after make build)
	@test -n "$(RPM)" || { echo "No .rpm yet; run make build first."; exit 1; }
	sudo dnf install $(RPM)

clean-sandbox: ## Forget the sandbox config, and its passwords in the keyring
	@if [ -f $(SANDBOX)/profiles.toml ] && command -v secret-tool > /dev/null; then \
	  for id in $$(sed -n 's/^id = "\(.*\)"$$/\1/p' $(SANDBOX)/profiles.toml); do \
	    secret-tool clear service maqtt username "$$id" || true; \
	  done; \
	fi
	rm -rf $(SANDBOX)
