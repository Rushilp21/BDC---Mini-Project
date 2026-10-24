import csv
import hashlib
import json
from pathlib import Path


def create(root=Path('.')):
    root = Path(root)
    sample = root / 'data/raw/hibp_sample.csv'
    origin = 'Fixed public synthetic demo strings'
    selected = []
    if sample.exists():
        with sample.open(newline='') as f:
            for r in csv.DictReader(f):
                selected.append(r['hash'])
                if len(selected) == 3: break
        origin = 'Selected hashes from local HIBP range sample; fixture rows remain synthetic data-quality tests'
    if not selected:
        selected = [hashlib.sha1(f'public-fixture-{i}'.encode()).hexdigest().upper() for i in range(3)]
    rows = []
    for h in selected:
        rows.extend([(h, '10'), (h, '10'), (h.lower(), '10'), (' \t'+h+'\t ', '10')])
    rows.extend([('', '1'), ('   ', '1'), ('G'+selected[0][1:], '1'), (selected[0][:-1], '1'),
                 (selected[0]+'A', '1'), (selected[0], '-1'), (selected[0], 'nonnumeric')])
    controls = [hashlib.sha1(f'public-synthetic-control-{i}'.encode()).hexdigest().upper() for i in range(2)]
    rows.extend((h, '1') for h in controls)
    target = root / 'data/fixtures/cleaning_test.csv'
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['hash', 'count', 'source'])
        w.writerows((h, c, 'synthetic-cleaning-test') for h, c in rows)
    # Independent expected fixture-only oracle; normalization includes invalid rows.
    normalized = sum(h.strip().upper() != h for h, _ in rows)
    manifest = dict(purpose='Synthetic data-quality tests; not additional verified breaches', input_records=len(rows),
                    invalid_records=7, normalized_rows=normalized, unique_hashes=len(set(selected+controls)),
                    duplicates_removed=9, rejection_reasons=dict(missing_hash=2, invalid_hash=3, invalid_count=2),
                    selected_hashes=selected, origin=origin, overlaps_sample=sample.exists(),
                    combined_policy='Measure sample + fixture independently; overlapping selected hashes increase duplicates by 3. Counts use max, not sum.')
    target.with_suffix('.manifest.json').write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    return manifest


if __name__ == '__main__': create()
