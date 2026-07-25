#!/usr/bin/env bash
# Déploiement sur le VPS, à lancer depuis le Mac :  ./deploy/deploy.sh
set -euo pipefail

HOST="tely@vps.tely.info"
PORT=443

cd "$(dirname "$0")/.."

rsync -avz --delete -e "ssh -p $PORT" \
  --exclude cache --exclude .git --exclude __pycache__ --exclude .venv \
  ./ "$HOST:incendie-monitoring/"

ssh -p "$PORT" "$HOST" bash -s <<'EOF'
set -e
cd ~/incendie-monitoring
[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install -q -r requirements.txt
sudo cp deploy/incendie-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable incendie-monitor >/dev/null 2>&1 || true
sudo systemctl restart incendie-monitor
sleep 2
echo "--- santé du backend ---"
curl -s http://127.0.0.1:8081/api/health && echo
EOF

echo "Déployé. Reste la conf nginx (voir deploy/nginx.conf) si pas déjà faite."
