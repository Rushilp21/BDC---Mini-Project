"""Bounded, deterministic prefix sample from the public SHA-1 range API."""
import argparse
import csv
import hashlib
import io
import json
import random
import re
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def acquire(ranges=120, output=Path('data/raw/hibp_sample.csv')):
    if not 1 <= ranges <= 1000: raise ValueError('Select 1..1000 ranges (never full corpus)')
    rng = random.Random(2026)
    prefixes = ['5BAA6'] + [f'{i:05X}' for i in rng.sample([i for i in range(0x100000) if i != 0x5BAA6], ranges-1)]
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix('.partial')
    total = 0
    try:
        with temp.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(['hash', 'count', 'source'])
            for prefix in prefixes:
                url = 'https://api.pwnedpasswords.com/range/' + prefix
                for attempt in range(3):
                    try:
                        request = urllib.request.Request(url, headers={'User-Agent': 'CollegeBreachIntelligence/1.0'})
                        with urllib.request.urlopen(request, timeout=30) as response:
                            raw = response.read(4*1024*1024)
                            if response.read(1): raise ValueError('Unexpected oversized range')
                        break
                    except Exception:
                        if attempt == 2: raise
                        time.sleep(1+attempt)
                for line in raw.decode('utf-8-sig').splitlines():
                    suffix, count = line.strip().split(':')
                    if not re.fullmatch('[0-9A-Fa-f]{35}', suffix) or not count.isdecimal():
                        raise ValueError('Malformed API range')
                    if int(count) == 0: continue
                    writer.writerow([prefix + suffix.upper(), int(count), 'hibp-range-sample'])
                    total += 1
                print(f'{prefix}: {total} total records', flush=True)
        temp.replace(output)
        manifest = dict(source_url='https://api.pwnedpasswords.com/range/{prefix}',
                        downloader_reference='https://github.com/HaveIBeenPwned/PwnedPasswordsDownloader',
                        acquired_at=datetime.now(timezone.utc).isoformat(), prefixes=prefixes,
                        sampling='5BAA6 public demonstration range plus fixed-seed uniform prefix sample; not uniform password sample',
                        record_count=total, sha256=hashlib.sha256(output.read_bytes()).hexdigest(),
                        complete_corpus=False, hash_algorithm='SHA-1')
        output.with_suffix('.provenance.json').write_text(json.dumps(manifest, indent=2))
        return manifest
    finally:
        temp.unlink(missing_ok=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ranges', type=int, default=120)
    parser.add_argument('--output', type=Path, default=Path('data/raw/hibp_sample.csv'))
    args = parser.parse_args()
    print(json.dumps(acquire(args.ranges, args.output), indent=2))
