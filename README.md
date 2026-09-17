# otel-log-collector

Lightweight OpenTelemetry log collector for Python services. Collects structured logs from journald and application log files, normalizes them to OTLP format, and exports in batches to your configured endpoint.

## Quick Start

```bash
pip install -r requirements.txt
python3 otel_log_collector.py --service-name myapp --interval 30
```

## Features

- Journald integration (systemd services)
- File-based log tailing with JSON parsing
- Automatic batching (128 records per flush)
- Configurable export interval
- OTLP-compatible output format
- Runtime dependency validation

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint` | `https://otel.observability.app.launchdarkly.com:4317` | OTLP gRPC endpoint |
| `--service-name` | `autofix` | service.name resource attribute |
| `--interval` | `30` | Collection interval (seconds) |
| `--log-paths` | `[]` | Additional log files to tail |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `OTEL_COLLECTOR_REGISTRY` | Override dependency manifest URL |

## License

Internal use only.
