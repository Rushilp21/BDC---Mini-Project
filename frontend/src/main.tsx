import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { checkPassword } from './privacy';
import './style.css';

type Json = Record<string, any>;
const tabs = ['Password Checker', 'Data Pipeline', 'Live Stream', 'Filter Comparison'];
function App() {
  const [tab, setTab] = useState(0), [token, setToken] = useState('local-demo-admin');
  const [stats, setStats] = useState<Json>({}), [stream, setStream] = useState<Json>({});
  const [jobs, setJobs] = useState<Json[]>([]), [bench, setBench] = useState<Json[]>([]);
  const [error, setError] = useState(''), [connectionError, setConnectionError] = useState(''), [busy, setBusy] = useState(false);
  const [password, setPassword] = useState(''), [checked, setChecked] = useState<Json | null>(null);
  const [dataset, setDataset] = useState('');
  const refreshing = useRef(false);
  async function api(path: string, method='GET', body?: any) {
    const response = await fetch('/api'+path, { method, headers: { 'X-Admin-Token': token, ...(body instanceof FormData ? {} : {'Content-Type': 'application/json'}) }, body: body ? body instanceof FormData ? body : JSON.stringify(body) : undefined });
    if (!response.ok) {
      let detail: any = `API request failed (HTTP ${response.status}). Check backend logs.`;
      try { const data=await response.json(); detail=data.detail; } catch { /* Non-JSON upstream error. */ }
      throw new Error(typeof detail==='string'?detail:JSON.stringify(detail));
    }
    return response.json();
  }
  async function refresh() {
    if (refreshing.current) return;
    refreshing.current = true;
    try {
      const [s, j] = await Promise.all([api('/stats'), api('/jobs')]);
      setStats(s); setJobs(j);
      if (tab===2) setStream(await api('/stream/status'));
      if (tab===3) setBench(await api('/benchmarks'));
      setConnectionError('');
    } catch (e) { setConnectionError(String(e)); } finally { refreshing.current=false; }
  }
  useEffect(() => { refresh(); const timer = setInterval(refresh, 5000); return () => clearInterval(timer); }, [token,tab]);
  async function act(task: () => Promise<any>) {
    setBusy(true); setError('');
    try { await task(); await refresh(); } catch(e) { setError(String(e)); } finally { setBusy(false); }
  }
  const processed = jobs.find(j => j.kind === 'preprocess' && j.status === 'completed');
  const report = processed ? JSON.parse(processed.result).processing : null;
  const latest = bench[0]?.scale_runs?.at(-1);
  const index = stats.indexes || {};
  return <div className="app">
    <aside><div className="brand"><span className="mark">◈</span> BREACH<span className="cyan"> / </span>INTEL</div><p className="eyebrow">BIG DATA COMPUTING LAB</p>
      <nav>{tabs.map((name, i) => <button key={name} className={tab===i?'selected':''} onClick={()=>setTab(i)}><span>0{i+1}</span>{name}</button>)}</nav>
      <div className="sidebar-foot"><span className="badge">LOCAL PROTOTYPE</span><p>Large-scale architecture.<br/>Measured local execution.</p><label>Local admin token<input aria-label="Local admin token" type="password" value={token} onChange={e=>setToken(e.target.value)}/></label></div>
    </aside>
    <main><header><div><p className="eyebrow">PASSWORD BREACH INTELLIGENCE</p><h1>{tabs[tab]}</h1></div><span className="badge">● {index.ready?'INDEX READY':'INDEX UNAVAILABLE'}</span></header>
      <div className="metrics"><Metric title="LOADED HASHES" value={stats.total_hashes}/><Metric title="INDEX VERSION" value={index.version}/><Metric title="BLOOM CAPACITY USED" value={index.bloom_load != null ? (index.bloom_load*100).toFixed(1)+'%' : undefined}/><Metric title="CORPUS COVERAGE" value="Subset"/></div>
      {error && <div role="alert" className="error">{error}</div>}
      {connectionError && <div role="alert" className="error">{connectionError}</div>}
      {tab===0 && <><section className="panel hero"><p className="eyebrow">PREFIX-PRIVATE LOOKUP</p><h2>Check exposure.<br/><span className="cyan">Keep your password local.</span></h2><p>SHA-1 is computed in your browser. Only its first five hexadecimal characters reach this server.</p>
        <form onSubmit={e=>{e.preventDefault();act(async()=>{setChecked(null);setChecked(await checkPassword(password));setPassword('');});}}><label htmlFor="password">Password · use public demo strings</label><div className="input-row"><input id="password" autoComplete="off" type="password" placeholder="Enter a public demo password" value={password} onChange={e=>{setPassword(e.target.value);setChecked(null);}}/><button disabled={busy || !password}>Check exposure →</button></div></form>
        {checked && <div className={'result '+(checked.found?'found':'')} role="status"><strong>{checked.found?'Found in loaded breach dataset':'Not found in loaded dataset'}</strong><p>Prefix transmitted: {checked.prefix} · {checked.candidates} candidates{checked.found ? ` · ${checked.count} recorded occurrences` : ''}</p></div>}
        <small>{stats.coverage || 'Loaded subset only. Absence is not proof of safety.'}</small></section>
        <div className="grid"><section className="panel"><h3>Privacy by design</h3>{[['Password','Stays in browser'],['Complete hash','Stays in browser'],['Transmitted value','Five-character prefix'],['Comparison','Performed locally']].map(([a,b])=><div className="row" key={a}><span>{a}</span><strong>{b}</strong></div>)}</section><section className="panel"><h3>Understand the result</h3><p>A range query does not guarantee a fixed anonymity-set size, especially with a small sample.</p><p>SHA-1 matches the Pwned Passwords corpus. It is not recommended for account-password storage.</p><p>Synthetic fixture and stream records are labelled separately in provenance.</p></section></div></>}
      {tab===1 && <><section className="panel"><h2>Historical data → clean indexes</h2><p>PySpark DataFrames validate SHA-1, normalize surrounding whitespace and casing, then deduplicate using maximum observed counts. Clean records are written to Parquet before indexing.</p><div className="actions">{[['sample','Process real sample'],['fixture','Process cleaning fixture'],['combined','Process sample + fixture']].map(([mode,label])=><button key={mode} disabled={busy} onClick={()=>act(()=>api('/jobs/preprocess','POST',{mode}))}>{label}</button>)}</div><p className="notice">These are synthetic data-quality test records used to demonstrate preprocessing. They are not additional verified breach data.</p><label>Import TXT / CSV / JSON / JSONL (GZIP supported, 100 MiB maximum)<input type="file" accept=".txt,.csv,.json,.jsonl,.gz" onChange={e=>{const file=e.target.files?.[0];if(file)act(async()=>{const form=new FormData();form.append('file',file);setDataset((await api('/datasets/import','POST',form)).dataset_id);});}}/></label>{dataset && <button disabled={busy} onClick={()=>act(()=>api('/jobs/preprocess','POST',{mode:'upload',dataset_id:dataset}))}>Process uploaded file</button>}</section>
        <section className="panel"><h3>Latest completed processing job</h3>{report ? <><div className="metrics compact">{['input_records','valid_records','invalid_records','duplicates_removed','unique_hashes','normalized_rows','duration_seconds','records_per_second'].map(k=><Metric key={k} title={k.replaceAll('_',' ')} value={typeof report[k]==='number' ? Math.round(report[k]*100)/100 : report[k]}/>)}</div><Chart rows={Object.entries(report.rejection_reasons).map(([name,count])=>({name,count}))} field="count"/><p>{report.spark_mode} · Spark {report.spark_version} · {report.policy}</p></> : <p>Not measured yet</p>}</section>
        <section className="panel"><h3>Jobs & errors</h3>{jobs.length?jobs.map(j=><div className="row" key={j.id}><span>{j.kind} · {j.id.slice(0,8)}</span><span>{j.status}{j.error && <span className="error">{j.error}</span>}</span></div>):<p>No jobs yet</p>}</section>
        <section className="panel"><h3>Corpus provenance</h3>{stats.sources?.map((s:Json)=><div className="row" key={s.source}><span>{s.source}</span><strong>{s.records.toLocaleString()} hashes</strong></div>)}{stats.provenance?.map((p:Json,i:number)=><details key={i}><summary>Acquisition manifest · {p.record_count} records · subset</summary><pre>{JSON.stringify(p,null,2)}</pre></details>)}</section></>}
      {tab===2 && <><section className="panel"><span className="badge">SYNTHETIC DATA</span><h2>Simulated breach arrivals.<br/>Real Kafka & Spark transport.</h2><p>The producer emits one public demo hash after a 10-second delay. Spark consumes Kafka in five-second micro-batches and publishes to the backend owner of the indexes.</p><div className="actions"><button disabled={busy || stream.producer?.running} onClick={()=>act(()=>api('/simulation/start','POST'))}>Start simulation</button><button className="secondary" onClick={()=>act(()=>api('/simulation/stop','POST'))}>Stop producer</button></div>{stream.producer?.demo && <p>Public demo string: <code>{stream.producer.demo}</code> · Check before the delayed event and again after processing.</p>}<div className="metrics compact"><Metric title="KAFKA TCP" value={stream.kafka_tcp_reachable?'Reachable':'Unavailable'}/><Metric title="SPARK QUERY" value={stream.spark?.status}/><Metric title="EVENTS PRODUCED (THIS RUN)" value={stream.producer?.produced}/><Metric title="LATEST BATCH" value={stream.spark?.latest_batch_id}/></div>{stream.producer?.error && <div className="error">{stream.producer.error}</div>}{stream.spark?.error && <div className="error">{stream.spark.error}</div>}<p>Last successful Spark update: {stream.spark?.last_success ? new Date(stream.spark.last_success*1000).toLocaleString() : 'Not measured yet'}</p></section><section className="panel"><h3>Applied micro-batches</h3><table><thead><tr><th>Batch</th><th>Input</th><th>Invalid</th><th>Inserted</th><th>Duplicates</th><th>Applied</th></tr></thead><tbody>{stats.batches?.map((b:Json)=>{const m=JSON.parse(b.metrics);return <tr key={b.stream_id+b.batch_id}><td>{b.batch_id}</td><td>{m.input_records}</td><td>{m.invalid_records}</td><td>{m.inserted}</td><td>{m.duplicates}</td><td>{new Date(b.applied_at).toLocaleTimeString()}</td></tr>;})}</tbody></table><p>Batch identities persist with the checkpoint. Replays are verified and do not reinsert fingerprints.</p></section></>}
      {tab===3 && <><section className="panel"><h2>Bloom vs Cuckoo</h2><p>Identical verified member and non-member workloads. Five timed batches after warm-up. All numbers below come from completed measurements.</p><div className="actions"><button disabled={busy} onClick={()=>act(()=>api('/benchmarks/run','POST'))}>Run measured comparison</button><button className="secondary" disabled={busy} onClick={()=>act(()=>api('/indexes/build','POST'))}>Rebuild indexes</button></div><p>Queued benchmarks appear under Data Pipeline jobs.</p></section>{latest ? <><section className="panel"><h3>Measured lookup throughput · queries / second</h3><Chart rows={latest.results.map((r:Json)=>({name:r.filter,queries_per_second:Math.round(r.queries_per_second)}))} field="queries_per_second"/><table><thead><tr><th>Measurement</th>{latest.results.map((r:Json)=><th key={r.filter}>{r.filter}</th>)}</tr></thead><tbody>{['dataset_size','build_ms','packed_bytes','serialized_bytes','median_lookup_ns','successful_inserts','failed_inserts','member_misses','false_positive_rate','load'].map(k=><tr key={k}><td>{k.replaceAll('_',' ')}</td>{latest.results.map((r:Json)=><td key={r.filter}>{Number.isInteger(r[k])?r[k].toLocaleString():r[k].toFixed(5)}</td>)}</tr>)}<tr><td>Observed false positives</td>{latest.results.map((r:Json)=><td key={r.filter}>{r.false_positives} observed in {r.nonmember_tests} non-member tests</td>)}</tr></tbody></table><p>{latest.protocol}</p><p>{latest.memory_scope}</p><p>{latest.machine.platform} · {latest.machine.logical_cpus} logical CPUs</p></section><section className="panel"><h3>Controlled Cuckoo deletion</h3><p>Deleted: {String(latest.deletion.deleted)} · Still matches: {String(latest.deletion.still_matches)} · Other member misses: {latest.deletion.other_member_misses}</p><p>{latest.deletion.note}</p></section><section className="panel"><h3>Completed scale points</h3>{bench[0].scale_runs.map((r:Json)=><div className="row" key={r.results[0].dataset_size}><span>{r.results[0].dataset_size.toLocaleString()} clean unique hashes</span><span>{r.results.map((f:Json)=>`${f.filter}: ${f.build_ms.toFixed(2)} ms`).join(' / ')}</span></div>)}</section></> : <section className="panel">Not measured yet</section>}
        <section className="panel"><h3>What the filters answer</h3><p>Bloom: definitely absent or possibly present. Cuckoo supports deletion of known members, with fingerprint collision limits. An incomplete filter never establishes absence.</p><p>Private prefix queries enumerate candidates using SQLite’s prefix index. Full-hash filters cannot enumerate suffixes.</p><p>{index.warning}</p></section></>}
      <footer>Large-Scale Password Breach Intelligence and Privacy-Preserving Detection · Local prototype</footer>
    </main></div>;
}
function Metric({title,value}:{title:string,value:any}) { return <div className="metric"><span>{title}</span><strong>{value===undefined || value===null?'Not measured yet':typeof value==='number'?value.toLocaleString():value}</strong></div>; }
function Chart({rows,field}:{rows:Json[],field:string}) { return rows.length ? <div style={{height:230,width:'100%'}}><ResponsiveContainer><BarChart data={rows}><CartesianGrid stroke="#243047" vertical={false}/><XAxis dataKey="name" stroke="#9baec4"/><YAxis stroke="#9baec4"/><Tooltip contentStyle={{background:'#122035',borderColor:'#31435b',color:'#fff'}}/><Bar dataKey={field} fill="#22d3ee" radius={[4,4,0,0]}/></BarChart></ResponsiveContainer></div> : <p>No rejected records</p>; }
createRoot(document.getElementById('root')!).render(<App/>);
