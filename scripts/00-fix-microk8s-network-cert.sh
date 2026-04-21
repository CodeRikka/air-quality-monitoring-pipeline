#!/usr/bin/env bash
set -euo pipefail

# Auto-fix helper for MicroK8s certificate mismatch after network changes.
# Typical symptom:
#   x509: certificate is valid for <old-ip>, not <new-ip>

if ! command -v microk8s >/dev/null 2>&1; then
  echo "microk8s command not found"
  exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl command not found"
  exit 1
fi

CURRENT_NODE_IP="$(microk8s kubectl get node "$(microk8s kubectl get nodes -o jsonpath='{.items[0].metadata.name}')" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
KUBELET_CERT="/var/snap/microk8s/current/certs/kubelet.crt"

echo "Detected node InternalIP: ${CURRENT_NODE_IP}"

if sudo test -f "${KUBELET_CERT}"; then
  if sudo openssl x509 -in "${KUBELET_CERT}" -text -noout | grep -q "${CURRENT_NODE_IP}"; then
    echo "kubelet certificate SAN already includes current IP, forcing cert refresh anyway."
  else
    echo "kubelet certificate SAN does not include current IP, refreshing certificates."
  fi
else
  echo "kubelet certificate file not found at ${KUBELET_CERT}, forcing cert refresh."
fi

echo "Refreshing MicroK8s certificates..."
if sudo microk8s refresh-certs --cert server.crt >/dev/null 2>&1; then
  echo "Refreshed server.crt"
elif sudo microk8s refresh-certs -e server.crt >/dev/null 2>&1; then
  echo "Refreshed server.crt (legacy flag)"
else
  sudo microk8s refresh-certs
fi

echo "Restarting MicroK8s..."
sudo microk8s stop
sudo microk8s start
microk8s status --wait-ready

echo "Restarting network/system components..."
microk8s kubectl rollout restart deployment/calico-kube-controllers -n kube-system || true
microk8s kubectl rollout restart deployment/metrics-server -n kube-system || true
microk8s kubectl delete pod -n kube-system -l k8s-app=calico-node || true

echo "Waiting for kube-system pods..."
sleep 10
microk8s kubectl get pods -n kube-system

echo "Done. If TLS errors persist, run:"
echo "  sudo microk8s refresh-certs"
echo "  sudo microk8s stop && sudo microk8s start"
