#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! docker compose version >/dev/null 2>&1; then
  echo 'Docker Compose v2 is required. Install Docker Desktop or the Compose plugin first.' >&2
  exit 1
fi

if [[ ! -e .env ]]; then
  umask 077
  random_password() {
    LC_ALL=C od -An -N24 -tx1 /dev/urandom | tr -d ' \n'
  }
  cat > .env <<EOF
# Local demo only. The web service is bound to 127.0.0.1 in compose.yaml.
FLUXPLANE_DB_PASSWORD=$(random_password)
FLUXPLANE_DEMO_DB_PASSWORD=$(random_password)
FLUXPLANE_API_KEY=
FLUXPLANE_HTTP_ALLOWED_HOSTS=demo-source
FLUXPLANE_WEB_PORT=8088
EOF
  echo 'Created local demo configuration in .env.'
else
  echo 'Using existing .env without changing it.'
  if ! grep -Eq '^FLUXPLANE_HTTP_ALLOWED_HOSTS=.*demo-source' .env; then
    echo 'Add demo-source to FLUXPLANE_HTTP_ALLOWED_HOSTS in .env, then run this script again.' >&2
    exit 1
  fi
fi

docker compose --profile demo up --build -d --wait
if docker compose exec -T api python examples/full_demo.py --phase verify >/dev/null 2>&1; then
  echo 'Demo data is already available.'
else
  docker compose exec -T api python examples/full_demo.py --phase seed
fi

port=$(sed -n 's/^FLUXPLANE_WEB_PORT=//p' .env | tail -1)
echo
echo "Open http://127.0.0.1:${port:-8088}"
if grep -Eq '^FLUXPLANE_API_KEY=.+$' .env; then
  echo 'This instance uses an API key from your existing .env; enter it in the console.'
else
  echo 'The local demo needs no login or model API key. Try the sample queries in Analysis Workspace.'
fi
