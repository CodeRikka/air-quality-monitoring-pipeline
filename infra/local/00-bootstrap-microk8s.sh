#!/usr/bin/env bash
set -euo pipefail

sudo snap install microk8s --classic --channel=1.35/stable
sudo usermod -a -G microk8s "$USER"
newgrp microk8s <<'EOF'
microk8s status --wait-ready
microk8s enable dns ingress helm3 metrics-server hostpath-storage
EOF
