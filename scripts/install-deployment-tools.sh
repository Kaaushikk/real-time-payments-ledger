#!/usr/bin/env bash
# Portable Linux tools for CI; no system-wide installation.
set -euo pipefail
mkdir -p "${1:?provide a tools directory}"
tool_dir="$(cd "$1" && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
cd "$work_dir"
helm_archive=helm-v4.3.2-linux-amd64.tar.gz
curl -fsSL --retry 3 "https://get.helm.sh/$helm_archive" -o "$helm_archive"
curl -fsSL --retry 3 "https://get.helm.sh/$helm_archive.sha256sum" -o "$helm_archive.sha256sum"
sha256sum -c "$helm_archive.sha256sum"
tar -xzf "$helm_archive" linux-amd64/helm
cp linux-amd64/helm "$tool_dir/helm"
base=https://github.com/yannh/kubeconform/releases/download/v0.8.0
curl -fsSL --retry 3 "$base/kubeconform-linux-amd64.tar.gz" -o kubeconform-linux-amd64.tar.gz
curl -fsSL --retry 3 "$base/CHECKSUMS" -o CHECKSUMS
sha256sum -c --ignore-missing CHECKSUMS
tar -xzf kubeconform-linux-amd64.tar.gz kubeconform
cp kubeconform "$tool_dir/kubeconform"
"$tool_dir/helm" version --short
"$tool_dir/kubeconform" -v
