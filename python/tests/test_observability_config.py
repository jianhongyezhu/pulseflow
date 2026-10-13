import json
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROMETHEUS_CONFIG = REPOSITORY_ROOT / "infra/prometheus/prometheus.yml"
ALERT_RULES = REPOSITORY_ROOT / "infra/prometheus/alerts.yml"
GRAFANA_DATASOURCE = (
    REPOSITORY_ROOT / "infra/grafana/provisioning/datasources/prometheus.yml"
)
GRAFANA_DASHBOARD_PROVIDER = (
    REPOSITORY_ROOT / "infra/grafana/provisioning/dashboards/pulseflow.yml"
)
GRAFANA_DASHBOARD = REPOSITORY_ROOT / "infra/grafana/dashboards/pulseflow-platform.json"


def test_prometheus_scrape_targets_and_alerts_match_platform():
    config = yaml.safe_load(PROMETHEUS_CONFIG.read_text(encoding="utf-8"))
    targets = {
        job["job_name"]: job
        for job in config["scrape_configs"]
    }

    assert targets["ingestion-api"]["metrics_path"] == "/actuator/prometheus"
    assert targets["ingestion-api"]["static_configs"][0]["targets"] == ["ingestion-api:8080"]
    assert targets["spark-streaming"]["static_configs"][0]["targets"] == [
        "host.docker.internal:9108"
    ]

    rules = yaml.safe_load(ALERT_RULES.read_text(encoding="utf-8"))["groups"][0]["rules"]
    alerts = {rule["alert"]: rule for rule in rules}
    assert alerts["PulseFlowApiDown"]["expr"] == 'up{job="ingestion-api"} == 0'
    assert alerts["PulseFlowApiDown"]["for"] == "30s"
    assert alerts["PulseFlowStreamingStopped"]["expr"] == 'up{job="spark-streaming"} == 0'
    assert alerts["PulseFlowStreamingStopped"]["for"] == "60s"


def test_grafana_provisioning_and_dashboard_have_expected_panels():
    datasource = yaml.safe_load(GRAFANA_DATASOURCE.read_text(encoding="utf-8"))
    provider = yaml.safe_load(GRAFANA_DASHBOARD_PROVIDER.read_text(encoding="utf-8"))
    dashboard = json.loads(GRAFANA_DASHBOARD.read_text(encoding="utf-8"))

    assert datasource["datasources"][0]["uid"] == "pulseflow-prometheus"
    assert provider["providers"][0]["options"]["path"] == "/var/lib/grafana/dashboards"
    assert dashboard["uid"] == "pulseflow-platform"
    assert len(dashboard["panels"]) == 6
    assert {panel["id"] for panel in dashboard["panels"]} == set(range(1, 7))
    queries = [panel["targets"][0]["expr"] for panel in dashboard["panels"]]
    assert "http_server_requests_seconds_count" in queries[0]
    assert "histogram_quantile(0.95" in queries[1]
    assert "http_server_requests_seconds_bucket" in queries[1]
    assert 'status=~"5.."' in queries[2]
    assert "or vector(0)" in queries[2]
    assert queries[3:6] == [
        "pulseflow_stream_input_rows_per_second",
        "pulseflow_stream_processed_rows_per_second",
        "pulseflow_stream_batch_duration_ms",
    ]
