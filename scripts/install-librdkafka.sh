#!/usr/bin/env bash
set -euo pipefail

# Build and install librdkafka from source.
#
# Usage: ./scripts/install-librdkafka.sh [version]
#
# Why this exists: confluent-kafka ships binary wheels that bundle librdkafka,
# but only for released CPythons. On a release-candidate interpreter there is
# no matching wheel, so the sdist is built instead — and that build needs
# librdkafka *headers* at least as new as the confluent-kafka being built.
# Distribution packages lag far behind (Ubuntu 24.04 ships librdkafka 2.3.0),
# so `apt-get install librdkafka-dev` gets as far as the version guard in
# confluent_kafka.h and stops:
#
#     error: "confluent-kafka-python requires librdkafka v2.15.0 or later"
#
# The two projects release in lockstep, so with no argument the version comes
# from the confluent-kafka entry in uv.lock. One source of truth: bumping the
# dependency cannot leave a second number behind.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${1:-}"
prefix="${LIBRDKAFKA_PREFIX:-/usr/local}"

if [ -z "$version" ]; then
    version="$(python3 - "$repo_root/uv.lock" <<'PY'
import sys
import tomllib

with open(sys.argv[1], 'rb') as handle:
    lock = tomllib.load(handle)

versions = [p['version'] for p in lock['package'] if p['name'] == 'confluent-kafka']
if len(versions) != 1:
    sys.exit(f'expected one confluent-kafka entry in uv.lock, found {len(versions)}')
print(versions[0])
PY
)"
fi

# Root inside a container, an unprivileged user on a CI runner: both install
# into a system prefix, only one of them needs sudo. A prefix the current user
# can already write to (a test build, a user-local install) needs neither, and
# ldconfig only means anything for a prefix the dynamic linker searches.
sudo_cmd=""
if [ "$(id -u)" -ne 0 ] && [ ! -w "$(dirname "$prefix")" ] && [ ! -w "$prefix" ]; then
    sudo_cmd="sudo"
fi

echo "Building librdkafka v${version} into ${prefix}"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

curl -fsSL "https://github.com/confluentinc/librdkafka/archive/refs/tags/v${version}.tar.gz" \
    -o "$workdir/librdkafka.tar.gz"
tar -xzf "$workdir/librdkafka.tar.gz" -C "$workdir"

cd "$workdir/librdkafka-${version}"
# librdkafka's configure is mklove, not autotools: it feature-detects against
# what is installed, so SSL/SASL/zlib support depends on the dev headers being
# present. The callers install them; nothing here forces a reduced build, so
# the result matches what the published wheels bundle.
./configure --prefix="$prefix"
make -j"$(nproc)"
$sudo_cmd make install

# Refresh the linker cache, or the freshly installed shared object is found at
# compile time and missing at import time. Escalation is independent of the
# install above: a writable system prefix needs no sudo to install into but
# still needs a cache refresh. A user-local prefix has nothing to add, so a
# failure here is not worth stopping for.
if [ "$(id -u)" -eq 0 ]; then
    ldconfig
elif command -v sudo >/dev/null 2>&1; then
    sudo ldconfig || true
fi

echo "librdkafka v${version} installed"
