#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv /opt/bdc-venv
python3 -c "from pathlib import Path; Path('/tmp/bdc-requirements.txt').write_text('\n'.join(l for l in Path('backend/requirements.txt').read_text().splitlines() if not l.startswith('pyspark==')))"
/opt/bdc-venv/bin/pip install --timeout 60 --index-url https://pypi.tuna.tsinghua.edu.cn/simple -r /tmp/bdc-requirements.txt
echo 'Source scripts/wsl_env.sh before running backend, Spark or tests.'
