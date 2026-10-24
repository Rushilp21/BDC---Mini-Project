import csv
import json
import pytest
from scripts.create_cleaning_fixture import create

pytestmark=pytest.mark.spark

@pytest.fixture(scope='module')
def spark_session():
    from spark.batch_pipeline import session
    spark=session('Breach tests')
    yield spark
    spark.stop()

def test_fixture_manifest(spark_session,tmp_path):
    from spark.batch_pipeline import run
    manifest=create(tmp_path)
    report=run([tmp_path/'data/fixtures/cleaning_test.csv'],tmp_path/'clean')
    for key in ['input_records','invalid_records','normalized_rows','unique_hashes','duplicates_removed','rejection_reasons']:
        assert report[key]==manifest[key], (key,report,manifest)

def test_combined_overlap(spark_session,tmp_path):
    from spark.batch_pipeline import run
    sample=tmp_path/'data/raw/hibp_sample.csv'
    sample.parent.mkdir(parents=True)
    with sample.open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['hash','count','source'])
        for i in range(3):w.writerow([f'{i:040X}',100,'hibp-test-reference'])
    m=create(tmp_path)
    report=run([sample,tmp_path/'data/fixtures/cleaning_test.csv'],tmp_path/'combined')
    assert report['duplicates_removed']==m['duplicates_removed']+3
    assert report['unique_hashes']==5

@pytest.mark.parametrize('ext',['txt','csv','json','jsonl'])
def test_formats(spark_session,tmp_path,ext):
    from spark.batch_pipeline import run
    p=tmp_path/('input.'+ext)
    valid='a'*40
    if ext=='txt':p.write_text(f'  {valid}  :3\n{valid}:4\nbroken:5\n{valid}:-1\n')
    elif ext=='csv':
        with p.open('w',newline='') as f:
            w=csv.writer(f);w.writerow(['hash','count','source']);w.writerows([(valid,3,'synthetic-test'),(valid,4,'synthetic-test'),('broken',5,'synthetic-test'),(valid,'-1','synthetic-test')])
    else:
        rows=[dict(hash=valid,count=3,source='synthetic-test'),dict(hash=valid,count=4,source='synthetic-test'),dict(hash='broken',count=5),dict(hash=valid,count=-1)]
        p.write_text(json.dumps(rows) if ext=='json' else '\n'.join(json.dumps(r) for r in rows))
    r=run([p],tmp_path/'out')
    assert r['input_records']==4
    assert r['unique_hashes']==1
    assert r['duplicates_removed']==1
    assert r['invalid_records']==2

def test_missing_counts_and_internal_whitespace(spark_session,tmp_path):
    from spark.batch_pipeline import run
    p=tmp_path/'input.jsonl'
    rows=[dict(hash='A'*40),dict(hash='A'*40,count=None),dict(hash='A'*20+' '+'A'*20,count=1),dict(hash='',count=1)]
    p.write_text('\n'.join(json.dumps(r) for r in rows))
    r=run([p],tmp_path/'out')
    assert r['invalid_records']==4
    assert r['rejection_reasons']==dict(invalid_count=2,invalid_hash=1,missing_hash=1)
