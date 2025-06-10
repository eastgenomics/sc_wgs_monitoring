#!/bin/bash

if [[ ! -d "/app/sc_wgs_monitoring/grafana" ]]; then
    mkdir -p /app/sc_wgs_monitoring/grafana
fi

if [[ -f "/app/sc_wgs_monitoring/grafana/sc_wgs_monitoring.prom" ]]; then
    rm /app/sc_wgs_monitoring/grafana/sc_wgs_monitoring.prom
fi

echo "SC WGS monitoring_cronjob_completed - $(date +%s)" > /app/sc_wgs_monitoring/grafana/sc_wgs_monitoring.prom