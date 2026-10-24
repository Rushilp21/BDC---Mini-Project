"""Generate the shareable team handoff from verified project evidence."""
import json
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Flowable
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import A4

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'output/pdf/Password_Breach_Intelligence_Team_Handoff.pdf'
OUT.parent.mkdir(parents=True, exist_ok=True)
pdfmetrics.registerFont(TTFont('Segoe', 'C:/Windows/Fonts/segoeui.ttf'))
pdfmetrics.registerFont(TTFont('SegoeBold', 'C:/Windows/Fonts/segoeuib.ttf'))
pdfmetrics.registerFont(TTFont('Consolas', 'C:/Windows/Fonts/consola.ttf'))
pdfmetrics.registerFontFamily('Segoe', normal='Segoe', bold='SegoeBold', italic='Segoe', boldItalic='SegoeBold')
NAVY = colors.HexColor('#11243A')
BLUE = colors.HexColor('#087E9D')
INK = colors.HexColor('#24384B')
MUTED = colors.HexColor('#52677B')
PALE = colors.HexColor('#EEF6F9')
LINE = colors.HexColor('#D6E1E8')
styles = {
    'body': ParagraphStyle('body', fontName='Segoe', fontSize=9.8, leading=13.4, textColor=INK, spaceAfter=6),
    'small': ParagraphStyle('small', fontName='Segoe', fontSize=8.6, leading=11.6, textColor=MUTED, spaceAfter=5),
    'h1': ParagraphStyle('h1', fontName='SegoeBold', fontSize=22, leading=26, textColor=NAVY, spaceAfter=12),
    'h2': ParagraphStyle('h2', fontName='SegoeBold', fontSize=12.5, leading=16, textColor=BLUE, spaceBefore=8, spaceAfter=5),
    'label': ParagraphStyle('label', fontName='SegoeBold', fontSize=8.4, leading=12, textColor=BLUE, spaceAfter=8),
    'cell': ParagraphStyle('cell', fontName='Segoe', fontSize=8.8, leading=11.7, textColor=INK),
    'head': ParagraphStyle('head', fontName='SegoeBold', fontSize=9.1, leading=12.5, textColor=colors.white),
    'code': ParagraphStyle('code', fontName='Consolas', fontSize=8.1, leading=12, textColor=NAVY, spaceAfter=8),
}
story = []
def p(text, style='body'):
    return Paragraph(text, styles[style])
def add(text, style='body'):
    story.append(p(text, style))
def h(text): add(text, 'h2')
def page(n, title, subtitle):
    if story: story.append(PageBreak())
    add(f'TEAM HANDOFF / {n:02d}', 'label')
    add(title, 'h1')
    add(subtitle, 'small')
    story.append(Spacer(1, 7))
def table(headers, rows, widths):
    data = [[p(x, 'head') for x in headers]] + [[p(str(x), 'cell') for x in r] for r in rows]
    t = Table(data, colWidths=widths, repeatRows=1, hAlign='LEFT')
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0),(-1,0), NAVY), ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),9),('RIGHTPADDING',(0,0),(-1,-1),9),
        ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white, PALE]),
        ('LINEBELOW',(0,0),(-1,0),.5,NAVY),('LINEBELOW',(0,1),(-1,-1),.4,LINE),
    ]))
    story.extend([t,Spacer(1,10)])
def callout(text):
    t=Table([[p(text)]],colWidths=[499])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),PALE),('BOX',(0,0),(-1,-1),.5,LINE),
                          ('LEFTPADDING',(0,0),(-1,-1),12),('RIGHTPADDING',(0,0),(-1,-1),12),
                          ('TOPPADDING',(0,0),(-1,-1),10),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story.extend([t,Spacer(1,9)])
def step(n,title,text):
    add(f'<b>{n}. {title}</b> {text}')
def code(text):
    add(escape(text).replace('\n','<br/>'), 'code')

class Pipeline(Flowable):
    def __init__(self, labels):
        super().__init__(); self.labels=labels;self.width=499;self.height=54
    def draw(self):
        c=self.canv; gap=13; w=(499-gap*(len(self.labels)-1))/len(self.labels)
        for i,label in enumerate(self.labels):
            x=i*(w+gap)
            c.setFillColor(PALE);c.setStrokeColor(LINE);c.roundRect(x,9,w,43,5,fill=1,stroke=1)
            c.setFillColor(NAVY);c.setFont('SegoeBold',8.8)
            lines=label.split('\n')
            for j,line in enumerate(lines):c.drawCentredString(x+w/2,35-j*12,line)
            if i<len(self.labels)-1:
                c.setStrokeColor(BLUE);c.line(x+w+2,30,x+w+gap-2,30)
                c.line(x+w+gap-5,33,x+w+gap-2,30);c.line(x+w+gap-5,27,x+w+gap-2,30)

page(1,'Password Breach Intelligence\nand Privacy-Preserving Detection',
     'Implementation summary and demonstration handbook | Prepared 24 October 2026')
add('A teammate guide to what is complete, how the application works, which examples to use, and what the results do and do not prove.')
callout('<b>Project scope:</b> A working local Big Data Computing prototype. It combines real breach-hash data, Spark cleaning, compact membership filters, private prefix checking, and synthetic incremental arrivals over real Kafka transport.')
h('What has been completed')
table(['Component','Completed implementation'],[
    ('Real dataset','236,495 HIBP SHA-1 records acquired from 120 bounded ranges, with source, acquisition date, selected prefixes and file checksum.'),
    ('Historical processing','Actual PySpark DataFrames; TXT/CSV/JSON/JSONL input, optional GZIP, validation, normalization, deduplication and clean Parquet output.'),
    ('Exact storage and indexes','SQLite with an indexed prefix column, plus genuine packed Bloom and bucket-based Cuckoo Filters.'),
    ('Private checker','Browser-local SHA-1; only a five-character prefix is sent; exact suffix comparison occurs locally.'),
    ('Incremental updates','Kafka KRaft broker, Spark Structured Streaming, checkpoint recovery and idempotent backend batch ingestion.'),
    ('Dashboard and validation','Four working sections, persisted jobs, real metrics/charts, measured benchmarks, automated tests and an isolated reset workflow.'),
],[118,381])
h('The two main workflows')
story.append(Pipeline(['Real hash\nsample','Spark\ncleaning','Clean\nParquet','SQLite +\nboth filters']))
story.append(Pipeline(['Synthetic\nproducer','Kafka\ntopic','Spark\nstreaming','Backend\nindex update']))
add('<b>Stack:</b> React + TypeScript + Vite, Tailwind CSS, Recharts, FastAPI/Pydantic, SQLite, PySpark and Apache Kafka.', 'small')
add('<b>Essential distinction:</b> The private checker retrieves candidates from SQLite. Bloom and Cuckoo Filters are maintained alongside the database for full-hash membership experiments; they cannot enumerate suffixes for a prefix.', 'small')

page(2,'What happens when\na password is entered', 'The password-checking path, using a fixed public example.')
step(1,'Enter a public demo string.', 'Type <b>password</b>. Typing alone does not issue a password-range query; the user must click <b>Check exposure</b>.')
step(2,'Hash inside the browser.', 'Web Crypto computes SHA-1 from the exact UTF-8 input. Password case and spaces are preserved.')
code('password\nSHA-1: 5BAA61E4C9B93F3F0682250B6CF8331B7EE68FD8\nPrefix: 5BAA6\nSuffix: 1E4C9B93F3F0682250B6CF8331B7EE68FD8')
step(3,'Send only the prefix.', 'The browser calls our local backend. It does not send the plaintext, complete client hash or client suffix.')
code('GET /api/range/5BAA6')
step(4,'Retrieve candidates.', 'SQLite returns suffixes and occurrence counts for loaded hashes beginning with that prefix. Our acquired 5BAA6 range contained 1,978 candidates.')
step(5,'Compare locally.', 'The browser compares its suffix with the returned candidates, displays found/not found in the loaded dataset, and clears the input after success.')
h('Why most other passwords return "not found"')
add('The sample contains only <b>120 of 1,048,576 possible prefixes</b>, approximately <b>0.0114%</b>. The range for <b>password</b> was deliberately included as a reliable real-data example. A common password can still be outside our selected ranges.')
table(['Public example','Observed result','Interpretation'],[
    ('password','Found','Real HIBP sample match.'),
    ('123456 / qwerty / admin / password123','Not found','Their prefixes are outside the selected sample ranges.'),
    ('public-synthetic-control-0','Found','Synthetic cleaning-fixture control.'),
    ('public-synthetic-control-1','Found','Synthetic cleaning-fixture control.'),
],[176,80,243])
callout('<b>Correct interpretation:</b> "Not found in our loaded dataset" is not a guarantee of safety. Queries search the locally loaded sample; they do not perform a fresh global HIBP search. Five-character prefix queries do not guarantee a fixed anonymity-set size.')
add('SHA-1 is used to match this corpus, not as a recommendation for account-password storage. No plaintext recovery or password cracking is performed.', 'small')

page(3,'Cleaning, duplicates\nand the second dataset', 'What Spark does, why counts change, and what is already loaded.')
h('Historical cleaning pipeline')
add('Spark reads the selected input, trims surrounding hash whitespace, converts hexadecimal letters to uppercase, validates full 40-character SHA-1 hashes and non-negative integer counts, rejects invalid rows, and deduplicates by normalized full hash. Internal malformed characters are not silently removed.')
add('For duplicate hashes with different counts, the policy is <b>maximum observed count</b>, not a sum of duplicated copies. Source labels are retained. Clean output is written to Parquet and then merged into SQLite and the filters.')
h('The cleaning fixture has already been processed')
add('The separate <b>cleaning_test.csv</b> has canonical rows plus exact duplicates, lowercase copies, surrounding spaces/tabs, empty hashes, invalid characters, wrong lengths and invalid counts. It is a synthetic data-quality exercise, not additional verified breach evidence.')
table(['Metric','Real sample','Fixture only','Sample + fixture'],[
    ('Input records','236,495','21','236,516'),
    ('Valid before deduplication','236,495','14','236,509'),
    ('Invalid records','0','7','7'),
    ('Duplicate valid rows removed','0','9','12'),
    ('Unique valid hashes','236,495','5','236,497'),
    ('Rows normalized','0','7','7'),
],[207,95,92,105])
add('<b>Fixture rejection breakdown:</b> 2 missing hashes, 3 invalid hashes and 2 invalid counts. Each rejected row has one primary reason. Normalized rows overlap validity categories and must not be added to the other totals.', 'small')
h('Why the fixture adds only two new hashes')
add('Its five unique valid hashes include <b>three selected from the real sample</b> and <b>two distinct synthetic controls</b>. Those three already exist in the main corpus. The initial fixture import therefore introduced only two additional exact records.')
callout('<b>Repeated imports are expected to add zero new records.</b> Cleaning metrics describe the selected input file. Database ingestion metrics describe whether those cleaned hashes already exist in the loaded store. Reprocessing the fixture still removes nine duplicate input rows even when it inserts zero new database rows.')
add('Combined cleaning removes 12 duplicates rather than 9 because the three selected real hashes overlap between the sample and fixture. Total loaded records may exceed the real-sample count because explicitly labelled test and stream records are also retained.', 'small')

page(4,'Bloom and Cuckoo Filters', 'Their role, implementation details and actual measured comparison.')
table(['Feature','Bloom Filter','Cuckoo Filter'],[
    ('Stores','Packed bits. Several calculated positions represent each inserted hash.','Short fingerprints in buckets. Each fingerprint has two candidate bucket locations.'),
    ('Membership answer','Any required zero bit means definitely absent; all bits set means possibly present.','A matching fingerprint means possibly present; no match means absent when the filter is complete.'),
    ('Deletion','Unsupported: clearing shared bits can damage other memberships.','Supported for a known inserted item. The demonstration uses a separate synthetic index.'),
    ('Our implementation','Deterministic BLAKE2b positions; standard Bloom sizing; target 1% FPR at provisioned capacity.','Four slots per bucket, 10-bit fingerprints, up to 500 relocations, rollback on failed insertion.'),
],[104,197,198])
add('A false positive means an absent item appears possibly present because bits or fingerprints collide. A positive filter answer alone is not exact proof. SQLite remains authoritative. On capacity/insertion failure, exact records are preserved and a larger filter can be rebuilt.', 'small')
h('Saved benchmark: 236,499 unique loaded hashes')
add('This snapshot includes the real sample plus labelled test records. Both filters received the same data and identical query workloads. Measurements used one warmup and five timed batches; per-query latency is derived from repeated batch timing.')
table(['Measured value','Bloom','Cuckoo'],[
    ('Packed index bytes','566,715 (about 553 KiB)','2,097,152 (2 MiB)'),
    ('Build time','1,799.75 ms','896.01 ms'),
    ('Median lookup latency','5.88 microseconds','4.28 microseconds'),
    ('Observed false positives','4 / 10,000 absent queries','12 / 10,000 absent queries'),
    ('Inserted member misses','0','0'),
    ('Failed insertions','0','0'),
],[207,146,146])
add('<b>Observed outcome:</b> Cuckoo was faster and Bloom used less packed storage in this configuration. Their FPR settings/load differ; this is not equal-FPR tuning or proof that one is universally better. Sizes exclude SQLite, Spark and the Python runtime.', 'small')
add('Measured scales were 10K, 100K and 236,499 hashes. All inserted members were checked. The controlled 100-item Cuckoo deletion succeeded with zero other-member misses; fingerprint collisions can still leave a deleted item matching.', 'small')
callout('<b>Presentation wording:</b> "We experimentally compare full-hash membership structures. Our privacy-enabled checker uses SQLite prefix retrieval and local exact comparison." Do not claim the Bloom Filter lists all suffixes for a prefix.')

page(5,'Kafka, Spark and\nincremental updates', 'Real transport and processing, with explicitly simulated breach arrivals.')
callout('<b>What is live?</b> Kafka and Spark process real messages. <b>What is simulated?</b> The new breach arrival. There is no automatic subscription to newly discovered real HIBP breaches in this prototype.')
story.append(Pipeline(['Public demo\nhash','Kafka\nbreach-hashes','Spark\nvalidation','Backend +\nindex update']))
step(1,'Start simulation.', 'The backend chooses a deterministic public demo string absent from the current store and delays its event by ten seconds.')
step(2,'Produce one event.', 'The producer sends JSON with event ID, full synthetic SHA-1, count, source and emission time to Kafka. Its source is synthetic-stream-demo.')
step(3,'Process a micro-batch.', 'Spark Structured Streaming consumes Kafka, applies validation/normalization and sends a cleaned batch to the internal ingestion endpoint. The trigger is about five seconds; actual latency can be longer.')
step(4,'Publish the update.', 'The backend deduplicates against SQLite, updates the prefix index and both filters, and advances the index version. Spark checkpoints allow recovery; repeated batch IDs/checksums do not duplicate insertion.')
h('Latest recorded before/after proof')
proof=json.loads((ROOT/'artifacts/metrics/latest-stream-proof.json').read_text())
table(['Evidence','Before','After'],[
    (escape(proof['public_synthetic_string']),'Not found','Found'),
    ('Total loaded hashes',f"{proof['total_before']:,}",f"{proof['total_after']:,}"),
    ('Index version',str(proof['version_before']),str(proof['version_after'])),
],[291,104,104])
add(f"One Kafka event was produced; Spark was running and applied batch <b>{proof['last_batch']}</b>. These are recorded test observations, not a promise that background processes will stay running after a terminal closes.", 'small')
h('How to demonstrate it reliably')
add('Open two dashboard tabs: <b>Live Stream</b> in one and <b>Password Checker</b> in the other. Start simulation, copy the newly displayed public string, and check it within the ten-second delay. It should be absent. Wait for an applied batch and increased index version, then check the same string again: it should be found.')
add('Use a new displayed string on every run. The example above is already loaded. If the initial ten-second window is missed, run another simulation rather than deleting real corpus records.')
add('<b>The producer stopping after one event is normal.</b> Spark should remain running, waiting for later messages. Kafka TCP reachability alone is insufficient proof: also show the applied batch and changed lookup result.', 'small')

page(6,'Show that every section works', 'An 8-10 minute walkthrough with observable evidence.')
table(['Dashboard section','Action','Evidence to show'],[
    ('Password Checker\n1 minute','Check password, then public-synthetic-control-0. Explain real vs synthetic sources.','Both are found. An unrepresented common password can be not found because coverage is limited.'),
    ('Privacy\n1 minute','Open browser Developer Tools > Network. Filter /range/ and check password.','GET /api/range/5BAA6, with no request body containing plaintext, complete hash or client suffix.'),
    ('Data Pipeline: real\n1 minute','Click Process real sample; wait for the completed job.','236,495 input/unique; zero invalid and cleaning duplicates for this acquired sample.'),
    ('Data Pipeline: fixture\n1 minute','Click Process cleaning fixture. Show the fixture file and manifest.','21 input, 7 invalid, 9 duplicate valid rows removed, 7 normalized and 5 unique.'),
    ('Combined data\n1 minute','Click Process sample + fixture. Explain the overlap.','236,516 input, 7 invalid, 12 duplicates removed and 236,497 unique.'),
    ('Live Stream\n2 minutes','Use two tabs for the absent -> found demonstration from page 5.','Producer acknowledgment, applied Spark batch, one new exact record and increased index version.'),
    ('Filter Comparison\n1-2 minutes','Run measured comparison and wait for its background job.','Actual sizes/timings, member misses = 0, failed inserts = 0 and false-positive counts with denominators.'),
    ('Cuckoo deletion\n30 seconds','Show the controlled deletion result.','Deleted = true, other-member misses = 0. Genuine breach records are not removed.'),
],[121,187,191])
h('Keep the evidence consistent')
add('Top-level loaded totals count all stored unique hashes. A processing report counts the selected files. Benchmark results describe the snapshot when that benchmark ran. Those numbers can legitimately differ after additional synthetic arrivals or repeated imports.')
add('"Zero newly inserted" on a repeated import is evidence of exact-store deduplication, not a failed cleaning job. "Zero observed false positives in N tests", if it occurs in a future run, is not zero probability.', 'small')
h('Automated proof commands in the prepared WSL environment')
code("wsl -d BDC-Spark -- bash -c 'source /mnt/d/BDC/scripts/wsl_env.sh;\npython scripts/healthcheck.py'")
code("wsl -d BDC-Spark -- bash -c 'source /mnt/d/BDC/scripts/wsl_env.sh;\npython scripts/check_stream_demo.py'")
add('The line breaks above are for readability: enter each wsl command as a single line. The stream proof saves artifacts/metrics/latest-stream-proof.json and uses a public generated string. It does not bypass Kafka by inserting directly into the database.', 'small')

page(7,'Team handoff and references', 'Files to know, startup instructions, validation and remaining limits.')
h('Restart the already prepared machine')
add('From PowerShell in <b>D:\\BDC</b>, run this in the first terminal and <b>keep it open</b> for Spark:')
code('wsl -d BDC-Spark -- bash /mnt/d/BDC/scripts/start_wsl_stack.sh')
add('In a second PowerShell terminal at D:\\BDC:')
code('cd frontend\nnpm run dev')
add('Use the URL Vite prints. Normal port: 5173; inspected session: 5174. API: 8000. WSL Kafka: 9094. The local development admin token is local-demo-admin unless changed. These localhost addresses work only on the machine running the project.', 'small')
h('Implementation map')
table(['Topic','Repository file'],[
    ('Browser hashing and local match','frontend/src/privacy.ts'),
    ('Bloom + Cuckoo implementations','backend/app/filters.py'),
    ('Exact storage and prefix retrieval','backend/app/store.py'),
    ('Historical Spark cleaning','spark/batch_pipeline.py'),
    ('Kafka + Spark micro-batches','spark/streaming_pipeline.py'),
    ('APIs, jobs and producer controls','backend/app/main.py'),
    ('Benchmarks / fixture generator','backend/app/benchmark.py / scripts/create_cleaning_fixture.py'),
],[185,314])
add('<b>Validation completed:</b> 22 backend/Spark regression tests, 2 real Kafka/Spark integration tests and 2 frontend privacy tests passed. The 10-test component rerun overlaps the regression suite and is not an additional independent test count. TypeScript/Vite build and Compose configuration checks passed.', 'small')
add('<b>Runtime limits:</b> Verified on isolated Ubuntu WSL2 with Python 3.10.12, Java 17.0.20.1, Spark 3.5.7 local[2] and Kafka 3.7.1. Docker image execution remains unverified because Docker Desktop failed to start its engine. A million-record run, multi-node scalability and production deployment hardening were not measured.', 'small')
add('<b>Storage note:</b> Active WSL SQLite is /opt/bdc-state/breach.sqlite inside the isolated runtime. Avoid simultaneous Windows/Linux access to a live SQLite WAL database. The prepared WSL distribution and runtime assets are local setup, not part of a source-only checkout. Use README.md and docs/LOCAL_WSL.md for fresh-machine setup.', 'small')
h('References for presentation slides')
add('1. Bloom, B. H. (1970). <i>Space/Time Trade-offs in Hash Coding with Allowable Errors.</i> Communications of the ACM. <link href="https://dl.acm.org/doi/10.1145/362686.362692" color="#087E9D">ACM paper</link>.<br/>'
    '2. Fan, B., Andersen, D. G., Kaminsky, M., and Mitzenmacher, M. D. (2014). <i>Cuckoo Filter: Practically Better Than Bloom.</i> CoNEXT. <link href="https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014-abstract.html" color="#087E9D">Authors\' publication page</link>.<br/>'
    '3. HIBP. <link href="https://haveibeenpwned.com/API/v3#SearchingPwnedPasswordsByRange" color="#087E9D">Pwned Passwords range-query documentation</link>.<br/>'
    '4. Apache Spark 3.5.7. <link href="https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html" color="#087E9D">Structured Streaming + Kafka integration</link>.', 'small')

def decorate(c, doc):
    width,height=A4
    c.saveState()
    c.setFillColor(BLUE);c.rect(0,height-8,width,8,fill=1,stroke=0)
    c.setFont('SegoeBold',8);c.setFillColor(MUTED)
    c.drawString(48,height-32,'BREACH INTELLIGENCE / IMPLEMENTATION HANDOFF')
    c.setStrokeColor(LINE);c.line(48,41,width-48,41)
    c.setFont('Segoe',8);c.drawString(48,27,'24 October 2026  |  Local prototype  |  Real sample + labelled synthetic tests')
    c.drawRightString(width-48,27,f'{doc.page}')
    c.restoreState()

doc=SimpleDocTemplate(str(OUT),pagesize=A4,rightMargin=48,leftMargin=48,topMargin=55,bottomMargin=57,
                      title='Password Breach Intelligence - Team Handoff',author='Project implementation team',
                      subject='Completed implementation, privacy flow, sample coverage, Kafka/Spark demo and measured results')
doc.build(story,onFirstPage=decorate,onLaterPages=decorate)
print(OUT)
