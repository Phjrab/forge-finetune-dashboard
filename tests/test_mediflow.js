// CPU client/DOM-stub verification; does not establish browser/GPU acceptance.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const context=vm.createContext({console,encodeURIComponent});
vm.runInContext(fs.readFileSync('static/mediflow.js','utf8'),context);
const Client=vm.runInContext('MediflowClient',context);
const id='mf-'+'a'.repeat(24),id2='mf-'+'b'.repeat(24);
const row={id,run_id:'old-text',job_id:'T1',attempt_id:'a1',campaign_id:'old',model:'model',task:'text',historical:true,source:'mediflow_historical',status:'succeeded',phase:'succeeded',optimizer_step:2,total_updates:2,train_loss:2,samples_processed:29,samples_total:29,stale:false,training_succeeded:true,reload_succeeded:null,evaluation_completed:null};
const step=(seq,value)=>({seq,optimizer_step:seq,train_loss:value,lr_used:null,provenance:'historical_reconstructed',emitted_at:null});
(async()=>{
  let calls=[],offline=false,rotation=false,resourceReads=0;
  const client=new Client(async path=>{
    calls.push(path);if(offline)throw new Error('offline');
    if(path==='/api/mediflow/health')return {connected:true};
    if(path.startsWith('/api/mediflow/campaigns?'))return {campaigns:[{campaign_id:'old'}],has_more:false};
    if(path.startsWith('/api/mediflow/runs?'))return {runs:[row],has_more:false};
    if(path.includes('/evaluations?'))return {evaluations:[]};
    if(path===`/api/mediflow/runs/${id}`)return {run:row,status:{},summary:null,artifacts:[]};
    const name=path.split('/').at(-1).split('?')[0],query=new URLSearchParams(path.split('?')[1]);
    if(name==='resources')resourceReads++;
    if(rotation && name==='steps' && query.get('after_seq')!=='0'){rotation=false;throw Object.assign(new Error('reset'),{status:409,code:'mediflow_cursor_reset'});}
    const after=Number(query.get('after_seq'));
    return {items:name==='steps'?(after?[step(2,2)]:[step(1,3),step(2,2)]):name==='resources'?[{seq:1,gpu_utilization_percent:null,availability:{gpu_utilization_percent:{available:false,reason:'sensor_unavailable'}}}]:[],next_after_seq:name==='steps'?2:name==='resources'?1:0,stream_epoch:name+'-epoch',has_more:false,telemetry_complete:null};
  });
  await client.poll([id]);assert.equal(client.runs.length,1);assert.equal(client.entry(id).streams.steps.items.length,2);assert.equal(client.entry(id).streams.resources.cursor,1);assert.equal(client.entry(id).streams.events.cursor,0);assert.equal(client.connected,true);
  await client.poll([id]);assert.equal(client.entry(id).streams.steps.items.length,2);assert.ok(calls.some(path=>path.includes('/steps?after_seq=2&limit=500&stream_epoch=steps-epoch')));
  offline=true;await assert.rejects(client.poll([id]));assert.equal(client.connected,false);assert.equal(client.entry(id).streams.steps.items.length,2);
  offline=false;await client.poll([id]);assert.equal(client.connected,true);assert.equal(client.entry(id).streams.steps.items.length,2);
  rotation=true;await client.poll([id]);assert.equal(client.entry(id).streams.steps.items.length,2);assert.equal(client.entry(id).streams.resources.cursor,1);assert.ok(resourceReads>=4);
  assert.throws(()=>client.accept(id,'steps',{stream_epoch:'steps-epoch',items:[step(2,99)],next_after_seq:2}),/integrity/);
  assert.equal(client.entry(id).streams.steps.items[1].train_loss,2);
  client.accept(id2,'steps',{stream_epoch:'other',items:[step(1,8)],next_after_seq:1});assert.equal(client.entry(id2).streams.steps.items.length,1);
  assert.equal(client.accept(id,'steps',{stream_epoch:'new-epoch',items:[],next_after_seq:0}),false);assert.equal(client.entry(id).streams.steps.cursor,0);assert.equal(client.entry(id).streams.resources.cursor,1);
  let pages=0;const paginated=new Client(async()=>++pages===1?{runs:[row],has_more:true,limit:500}:{runs:[{...row,id:id2}],has_more:false});assert.equal((await paginated.list('/api/mediflow/runs','runs')).length,2);
  const catchup=new Client(async path=>{const after=Number(new URLSearchParams(path.split('?')[1]).get('after_seq'));const end=Math.min(1200,after+500);return {items:Array.from({length:end-after},(_,index)=>step(after+index+1,1)),next_after_seq:end,stream_epoch:'many',has_more:end<1200};});
  await catchup.stream(id,'steps');assert.equal(catchup.entry(id).streams.steps.items.length,1200);assert.equal(catchup.entry(id).streams.steps.cursor,1200);
  const expired=new Client(async path=>{if(path.includes('/evaluations?'))throw Object.assign(new Error('expired'),{status:401});return {items:[],next_after_seq:0,stream_epoch:'empty',has_more:false};});await assert.rejects(expired.hydrate(id),error=>error.status===401);
  const evalPages=new Client(async path=>{if(path.includes('/evaluations?')){const offset=Number(new URLSearchParams(path.split('?')[1]).get('offset'));return {evaluations:Array.from({length:Math.min(500,620-offset)},(_,i)=>({evaluation_id:String(offset+i)})),offset,limit:500,total:620,has_more:offset+500<620};}return {items:[],next_after_seq:0,stream_epoch:'empty',has_more:false};});
  await evalPages.hydrate(id);assert.equal(evalPages.entry(id).evaluations.length,620);assert.equal(evalPages.entry(id).evaluations.at(-1).evaluation_id,'619');
  // Use the production application and actual DOM branches with a minimal stub.
  class Element{constructor(dataset={}){this.dataset=dataset;this.innerHTML='';this.textContent='';this.hidden=false;this.style={};this.value='';this.attributes={};this.classList={toggle(){}};}addEventListener(){}setAttribute(k,v){this.attributes[k]=v;}showModal(){}close(){}focus(){}scrollIntoView(){}querySelectorAll(){return [];}querySelector(){return null;}}
  const nodes=new Map(),get=s=>{if(!nodes.has(s))nodes.set(s,new Element());return nodes.get(s);};
  const groups={'.filter':['all','LLM','VLM'].map(filter=>new Element({filter})),'.tab':['logs','resources','evaluation','configuration','checkpoints'].map(tab=>new Element({tab})),'.nav-item':[]};
  const dom=vm.createContext({console,Date,JSON,Math,Number,String,Object,Array,Promise,AbortController,encodeURIComponent,localStorage:{getItem(){return null;},setItem(){}},setInterval(){},setTimeout(){return 1;},clearTimeout(){},document:{querySelector:get,querySelectorAll:s=>groups[s]||[],addEventListener(){}},fetch:async()=>({ok:true,json:async()=>({mode:'mediflow',runs:[]})})});
  const run=code=>vm.runInContext(code,dom);
  for(const file of ['i18n.js','mediflow.js','app.js','workspace.js','training.js','builder.js'])vm.runInContext(fs.readFileSync('static/'+file,'utf8'),dom);
  run("snapshot={mode:'mediflow',runs:[]};render()");assert.equal(get('#run-detail').hidden,true);assert.equal(get('#workbench-empty').hidden,false);
  run(`mediflow.runs=[${JSON.stringify(row)}];snapshot.runs=mediflow.runs;selectedId=mediflow.runs[0].id;mediflow.entry(selectedId).streams.steps.items=${JSON.stringify([step(1,3),step(2,2)])};render()`);
  assert.match(get('#runs-body').innerHTML,/Historical reconstruction/);assert.doesNotMatch(get('#runs-body').innerHTML,/Demo|Real GPU|r16|QLoRA/);assert.equal(get('#run-source').textContent,'Historical reconstruction');assert.ok(!get('#loss-chart').innerHTML.includes('NaN'));assert.match(get('#inspector-config').innerHTML,/Unknown/);assert.match(get('#run-controls').innerHTML,/disabled/);assert.doesNotMatch(get('#run-controls').innerHTML,/data-action/);
  run("currentTab='resources';render()");assert.match(get('#tab-content').innerHTML,/No measured series/);assert.doesNotMatch(get('#tab-content').innerHTML,/NaN|undefined/);
  assert.equal(run("mfComparability({}, {})"),'Comparison identity unknown');const identity={data_hash:'hash',protocol:'p',protocol_version:'v1',precision:'fp16',model:'m',task:'text',cohort_id:'c',split:'test'};assert.equal(run(`mfComparability(${JSON.stringify(identity)},${JSON.stringify(identity)})`),'Matching evaluation cohort');assert.equal(run(`mfComparability(${JSON.stringify(identity)},{...${JSON.stringify(identity)},precision:'bf16'})`),'Different evaluation cohort');
  const evaluation={variant:'tuned',dataset_id:'safe',status:'partial',scheduled:100,attempted:12,valid:9,invalid:1,failed:2,unattempted:88,metrics:{coverage:.09,answered_accuracy:.7,end_to_end_correct_fraction:.063},subgroups:[{label:'OPEN',metrics:{lexical_token_f1:.2}}],provenance:'historical_reconstructed'};
  const evaluationHTML=run(`renderMediflowEvaluations([${JSON.stringify(evaluation)}])`);for(const label of ['coverage','answered_accuracy','end_to_end_correct_fraction','Scheduled','Attempted','Invalid','Unattempted','OPEN','partial'])assert.ok(evaluationHTML.includes(label));
  assert.match(run("renderMediflowEvaluations([{variant:'base',status:'final',receipt_verified:null,metrics:{}}])"),/receipt not verified/);
  assert.match(run("mfChart([{seq:1,value:2},{seq:2,value:null},{seq:3,value:1}],'value','test')"),/d="M[^\"]+ M/);
  assert.match(run("mfChart([{seq:1,value:2},{seq:3,value:1}],'value','test')"),/d="M[^\"]+ M/);
  run("mediflow.runs[0].job_id='<img src=x onerror=evil()>';currentTab='logs';mediflow.entry(selectedId).streams.events.items=[{seq:1,event:'hold',message:'<script>evil()</script>',emitted_at:null}];render()");assert.doesNotMatch(get('#runs-body').innerHTML,/<img/);assert.doesNotMatch(get('#tab-content').innerHTML,/<script/);
  run("I18n.setLanguage('ko');render()");assert.match(get('#run-source').textContent,/과거/);assert.match(get('#run-controls').innerHTML,/MediFlow supervisor/);run("I18n.setLanguage('en')");
  await assert.rejects(run("api('/api/training/runs',{})"),/Execution controls/);await assert.rejects(run("api('/api/runs',{})"),/Execution controls/);assert.equal(run("builderLaunchReady('demo',{})"),false);assert.equal(run("builderLaunchReady('real',{})"),false);
  run("mediflow.connected=false;mfConnection()");assert.match(get('#connection').innerHTML,/Disconnected/);assert.ok(!get('#connection').innerHTML.includes('Worker stale'));
  run("mediflow.connected=true;mediflow.runs[0].stale=true;render();mfConnection()");assert.match(get('#connection').innerHTML,/connected/);assert.match(get('#run-observed').textContent,/Worker stale/);
  assert.match(fs.readFileSync('static/app.js','utf8'),/setInterval\(refresh,3000\)/);assert.match(fs.readFileSync('static/index.html','utf8'),/src="\/mediflow.js"/);
  console.log('PASS MediFlow independent cursors, replay/dedupe, rotation/reset, reconnect, registry pages, empty hydration, history/null/XSS, evaluation denominators/cohorts, KO/EN and legacy launch guards (CPU DOM stubs only).');
})().catch(error=>{console.error(error);process.exitCode=1;});
