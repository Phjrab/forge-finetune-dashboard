'use strict';
// This client is a read model. It never launches, signals, or owns a worker.
class MediflowClient {
  constructor(request) { this.request=request; this.runs=[];this.campaigns=[];this.health=null;this.cache=new Map();this.connected=false; }
  clear() {this.runs=[];this.campaigns=[];this.health=null;this.cache.clear();this.connected=false;mediflowCampaignFilter='';}
  entry(id) { if(!this.cache.has(id))this.cache.set(id,{streams:Object.fromEntries(['steps','resources','events'].map(name=>[name,{cursor:0,epoch:null,items:[],complete:null}])),evaluations:[],detail:null,errors:[]});return this.cache.get(id); }
  reset(id,name) { this.entry(id).streams[name]={cursor:0,epoch:null,items:[],complete:null}; }
  accept(id,name,page) {
    const state=this.entry(id).streams[name];
    if(state.epoch && page.stream_epoch!==state.epoch) { this.reset(id,name);return false; }
    state.epoch=page.stream_epoch;
    const known=new Map(state.items.map(item=>[item.seq,item]));
    for(const item of page.items||[]) {
      if(!Number.isSafeInteger(item.seq)||item.seq<1)continue;
      if(known.has(item.seq)) { if(JSON.stringify(known.get(item.seq))!==JSON.stringify(item))throw new Error(typeof t==='function'?t('Stream integrity conflict'):'Stream integrity conflict');continue; }
      if(item.seq<=state.cursor)continue;
      known.set(item.seq,item);
    }
    state.items=[...known.values()].sort((a,b)=>a.seq-b.seq).slice(-5000);
    state.cursor=Math.max(state.cursor,Number.isSafeInteger(page.next_after_seq)?page.next_after_seq:0,...state.items.map(item=>item.seq));
    state.complete=page.telemetry_complete??null;
    return true;
  }
  async stream(id,name) {
    // Bounded catch-up per poll; the next poll resumes from the last original seq.
    for(let n=0;n<4;n++) {
      const state=this.entry(id).streams[name];
      const query=`after_seq=${state.cursor}&limit=500${state.epoch?'&stream_epoch='+encodeURIComponent(state.epoch):''}`;
      let page;
      try { page=await this.request(`/api/mediflow/runs/${encodeURIComponent(id)}/${name}?${query}`); }
      catch(error) { if(error.status===409 && error.code==='mediflow_cursor_reset' && n===0){this.reset(id,name);continue;}throw error; }
      if(!this.accept(id,name,page))continue;
      if(!page.has_more)return;
    }
  }
  async list(path,key) {
    const rows=[];let offset=0;
    // Paginate registry metadata, never raw training examples.
    for(let n=0;n<100;n++) {const page=await this.request(`${path}${path.includes('?')?'&':'?'}offset=${offset}&limit=500`);rows.push(...(page[key]||[]));if(!page.has_more)break;offset+=page.limit||500;}
    return rows;
  }
  async hydrate(id) {
    const entry=this.entry(id);entry.errors=[];
    const tasks=[['detail',()=>this.request(`/api/mediflow/runs/${encodeURIComponent(id)}`).then(value=>{entry.detail=value;})],['evaluations',()=>this.list(`/api/mediflow/runs/${encodeURIComponent(id)}/evaluations`,'evaluations').then(value=>{entry.evaluations=value;})],...['steps','resources','events'].map(name=>[name,()=>this.stream(id,name)])];
    const results=await Promise.allSettled(tasks.map(([,task])=>task()));
    results.forEach((result,index)=>{if(result.status==='rejected')entry.errors.push({stream:tasks[index][0],message:result.reason.message});});
    const authorization=results.find(result=>result.status==='rejected'&&result.reason.status===401);if(authorization)throw authorization.reason;
    if(entry.errors.length)throw new Error(typeof t==='function'?t('MediFlow stream disconnected'):'MediFlow stream disconnected');
  }
  async poll(ids=[]) {
    try {
      const results=await Promise.allSettled([this.request('/api/mediflow/health'),this.list('/api/mediflow/campaigns','campaigns'),this.list('/api/mediflow/runs','runs')]);
      if(results.some(r=>r.status==='rejected'))throw results.find(r=>r.status==='rejected').reason;
      this.health=results[0].value;this.campaigns=results[1].value;this.runs=mfOrderRuns(results[2].value);
      const targets=[...new Set(ids.filter(id=>this.runs.some(run=>run.id===id)))];
      if(!targets.length && this.runs.length)targets.push(this.runs[0].id);
      const details=await Promise.allSettled(targets.map(id=>this.hydrate(id)));
      const authorization=details.find(result=>result.status==='rejected'&&result.reason.status===401);if(authorization)throw authorization.reason;
      this.connected=details.every(result=>result.status==='fulfilled') && this.health.connected!==false;
    }catch(error){this.connected=false;throw error;}
  }
}
let mediflowCampaignFilter='';
function mfOrderRuns(runs){
  const rank=run=>run.source==='mediflow_live'&&!run.historical?(run.status==='running'?0:1):typeof run.train_loss==='number'&&Number.isFinite(run.train_loss)?2:3;
  const identity=run=>[run.campaign_id,run.job_id,run.run_id,run.attempt_id].join(' · ');
  return [...runs].sort((a,b)=>rank(a)-rank(b)||(Date.parse(b.last_emitted_at)||0)-(Date.parse(a.last_emitted_at)||0)||identity(a).localeCompare(identity(b),undefined,{numeric:true}));
}
const mediflow = new MediflowClient((path,body)=>api(path,body));
function isMediflow(){return snapshot?.mode==='mediflow';}
const mfKnown=value=>value!==null && value!==undefined && value!=='';
const mfNumber=value=>typeof value==='number' && Number.isFinite(value);
function mfValue(value){return mfKnown(value)?String(value):t('Unknown');}
function mfBool(value){return t(value===true?'Verified':value===false?'Not verified':'Unknown');}
function mfSource(run){return t(run.historical===true||run.source==='mediflow_historical'?'Historical reconstruction':run.source==='mediflow_live'?'Live measurement':'Unknown source');}
function mfName(run){return [run.job_id||run.run_id||run.id,run.attempt_id].filter(Boolean).join(' · ');}
function mfKind(run){return run.task==='vqa'?'VLM':run.task==='text'?'LLM':t('Unknown');}
function mfTime(value){return value?escapeHtml(value):t('Original time unavailable');}
function mfReason(record,key){return record?.availability?.[key]?.reason||t('Not measured');}
function mfMetric(record,key,digits=3){return mfNumber(record?.[key])?fmt(record[key],digits):`— (${escapeHtml(mfReason(record,key))})`;}
function mfRows(rows){return `<dl class="mf-facts">${rows.map(([key,value])=>`<dt>${escapeHtml(t(key))}</dt><dd>${escapeHtml(mfValue(value))}</dd>`).join('')}</dl>`;}
function mfExport(id){return `<a class="button secondary" href="/api/mediflow/runs/${encodeURIComponent(id)}/export?format=json">JSON</a><a class="button secondary" href="/api/mediflow/runs/${encodeURIComponent(id)}/export?format=csv">CSV</a>`;}
function mfSteps(id){return mediflow.entry(id).streams.steps.items;}
function mfChart(items,key,label,xkey='seq') {
  if(!items.some(item=>mfNumber(item[key])&&mfNumber(item[xkey])))return `<div class="chart-empty">${escapeHtml(t('No measured series'))} · ${escapeHtml(t(label))}</div>`;
  const points=items.slice(-1500),values=points.filter(item=>mfNumber(item[key])).map(item=>item[key]),xs=points.map(item=>item[xkey]).filter(mfNumber);
  const lo=Math.min(...values),hi=Math.max(...values),span=hi-lo||Math.max(Math.abs(hi)*.1,1),xmin=Math.min(...xs),xmax=Math.max(...xs);
  const x=v=>45+(v-xmin)/(xmax-xmin||1)*635,y=v=>15+(hi-v)/span*120;
  let joined=false,previous=null;const path=points.map(item=>{if(!mfNumber(item[key])||!mfNumber(item[xkey])){joined=false;return '';}if(previous!==null && item[xkey]>previous+1)joined=false;if(mfNumber(item.sample_gap_seconds)&&mfNumber(item.sample_interval_seconds)&&item.sample_gap_seconds>item.sample_interval_seconds*2)joined=false;previous=item[xkey];const p=`${joined?'L':'M'}${x(item[xkey]).toFixed(2)},${y(item[key]).toFixed(2)}`;joined=true;return p;}).join(' ');
  return `<svg viewBox="0 0 700 170" role="img" aria-label="${escapeHtml(t(label))}"><text x="0" y="20">${escapeHtml(hi.toPrecision(3))}</text><text x="0" y="140">${escapeHtml(lo.toPrecision(3))}</text><text x="45" y="165">${escapeHtml(xmin)}</text><text x="680" y="165" text-anchor="end">${escapeHtml(xmax)}</text><path d="${path}" fill="none" stroke="var(--purple)" stroke-width="2"/></svg>`;
}
function mfConnection() {
  const ok=mediflow.connected;
  connectionLabel=ok?'MediFlow connected':'Disconnected · retained observations';
  $('#connection').className='connection '+(ok?'ok':'error');$('#connection').innerHTML=`<i></i> ${t(connectionLabel)}`;
}
async function refreshMediflow(){
  const ids=nav==='compare'?[$('#compare-a')?.value,$('#compare-b')?.value]:[selectedId];
  await mediflow.poll(ids);snapshot.runs=mediflow.runs;snapshot.campaigns=mediflow.campaigns;
  mfConnection();
}
function renderMediflowSource(){
  document.body?.setAttribute('data-backend','mediflow');
  if($('#tab-resources'))$('#tab-resources').hidden=false;
  $('.demo-notice').innerHTML=`<div><strong>${t('MediFlow · external engine')}</strong><span>${t('Execution belongs to the MediFlow supervisor. Forge displays durable observations.')}</span></div>`;
  for(const selector of ['#new-run','#empty-new','#revise-config']){const node=$(selector);if(node){node.disabled=true;node.title=t('Execution controls unavailable');}}
  for(const selector of ['.workspace small','footer span','.demo-tag','.local-badge'])if($(selector))$(selector).textContent=t('MediFlow · external engine');
}
function renderMediflow(){
  document.body?.setAttribute('data-view',nav);updateExplorerSize();renderMediflowSource();
  const runs=mfOrderRuns(mediflow.runs.length?mediflow.runs:(snapshot.runs||[]));
  const campaignRuns=runs.filter(run=>!mediflowCampaignFilter||run.campaign_id===mediflowCampaignFilter);
  if(!campaignRuns.some(run=>run.id===selectedId))selectedId=campaignRuns[0]?.id||null;
  const selected=campaignRuns.find(run=>run.id===selectedId);
  const advanced=!['overview','runs','artifacts'].includes(nav);
  $('#run-detail').hidden=advanced||!selected;$('#workbench-empty').hidden=advanced||Boolean(selected);
  $('#workbench-empty h2').textContent=t('No MediFlow runs registered');$('#workbench-empty p').textContent=t('Waiting for durable telemetry. No demo values are substituted.');
  $('#workspace-alerts').hidden=nav!=='overview';
  renderMediflowCampaigns(runs);
  for(const selector of ['#summary-total','#nav-count','#experiment-count'])$(selector).textContent=runs.length;
  $$('.filter').forEach(btn=>btn.setAttribute('aria-pressed',String(btn.dataset.filter===filter)));
  const shown=campaignRuns.filter(run=>filter==='all'||mfKind(run)===filter);
  $('#experiment-count').textContent=shown.length;
  $('#no-runs').textContent=t('No experiments in this view yet');$('#no-runs').hidden=shown.length>0;
  $('#runs-body').innerHTML=shown.map(run=>{const progress=mfNumber(run.optimizer_step)&&mfNumber(run.total_updates)&&run.total_updates>0?pct(run.optimizer_step/run.total_updates*100):null;return `<tr tabindex="0" data-run="${escapeHtml(run.id)}" class="${run.id===selectedId?'selected':''}" aria-label="${escapeHtml(t('Select')+' '+mfName(run))}"><td><div class="run-name"><span class="type-icon">${run.task==='vqa'?'▧':'≋'}</span><div>${escapeHtml(mfName(run))}<div class="run-meta">${escapeHtml(mfSource(run))} · ${escapeHtml(mfKind(run))} · ${escapeHtml(mfValue(run.model))}</div></div></div></td><td><span class="status ${escapeHtml(run.status||'unknown')}">${escapeHtml(t(run.status||'Unknown'))}</span>${run.stale?`<span class="pill">${t('Worker stale')}</span>`:''}<small>${escapeHtml(t(run.phase||'Unknown'))}</small></td><td>${progress===null?'—':`${fmt(progress,0)}%`}<small>${escapeHtml(mfValue(run.optimizer_step))} / ${escapeHtml(mfValue(run.total_updates))}</small></td><td>${fmt(run.train_loss)}</td><td>${escapeHtml(run.campaign_id)}</td><td>↗</td></tr>`;}).join('');
  $$('#runs-body tr[data-run]').forEach(row=>{row.onclick=()=>{selectRun(row.dataset.run);refresh();};row.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();row.onclick();}};});
  $('#mobile-run').innerHTML=campaignRuns.map(run=>`<option value="${escapeHtml(run.id)}">${escapeHtml(mfName(run))} · ${escapeHtml(mfSource(run))}</option>`).join('');$('#mobile-run').value=selectedId||'';
  $('#mobile-run-label').hidden=advanced;
  if(selected)renderMediflowDetail(selected);
  if(nav==='compare' && $('#compare-output'))updateMediflowComparison();
}
function mfCampaignSummary(c,runs){
  const subset=runs.filter(run=>run.campaign_id===c.campaign_id);
  const firstSeed=subset.some(run=>/^v6-first-seed-(?:T[1-6]|V[1-4])$/.test(run.job_id||''));
  return firstSeed?`${t('First seed complete')}: ${mfValue(c.first_seed_completed)} / ${mfValue(c.first_seed_expected)}`:`${t('Runs')}: ${mfValue(c.run_count??subset.length)}`;
}
function renderMediflowCampaigns(runs){
  const host=$('#workspace-alerts');
  if(!host.dataset.mfCampaignLayout){host.innerHTML=`<div class="mf-campaign-toolbar"><label><span id="mf-campaign-label"></span> <select id="mf-campaign-select" aria-label="${t('Campaign')}"></select></label><small id="mf-run-order"></small></div><div class="mf-campaigns" id="mf-campaign-summary"></div>`;host.dataset.mfCampaignLayout='true';}
  const select=$('#mf-campaign-select');
  $('#mf-campaign-label').textContent=t('Campaign');select.setAttribute('aria-label',t('Campaign'));
  const options=`<option value="">${t('All campaigns')}</option>`+(mediflow.campaigns||[]).map(c=>`<option value="${escapeHtml(c.campaign_id)}">${escapeHtml(c.campaign_id)}</option>`).join('');
  if(select.innerHTML!==options)select.innerHTML=options;
  if(select.value!==mediflowCampaignFilter)select.value=mediflowCampaignFilter;
  select.onchange=()=>{mediflowCampaignFilter=select.value;render();refresh();};
  $('#mf-run-order').textContent=t('Live runs first · historical records with loss first');
  $('#mf-campaign-summary').innerHTML=(mediflow.campaigns||[]).map(c=>`<div><strong>${escapeHtml(c.campaign_id)}</strong><span>${escapeHtml(mfCampaignSummary(c,runs))}</span><small>${escapeHtml(Object.entries(c.status_counts||{}).map(([key,value])=>`${t(key)}: ${value}`).join(' · '))}</small><small>${escapeHtml([...new Set(runs.filter(run=>run.campaign_id===c.campaign_id&&!['succeeded','failed','unknown'].includes(run.phase)).map(run=>t(run.phase)))].join(' · '))}</small><small>${mfTime(c.last_emitted_at)}</small></div>`).join('')||t('No MediFlow campaigns registered');
}
function renderMediflowDetail(run){
  const entry=mediflow.entry(run.id),detail=entry.detail||{},status=detail.status||{},summary=detail.summary||{},steps=entry.streams.steps.items,last=steps.at(-1)||{},resources=entry.streams.resources.items,resource=resources.at(-1)||{};
  $('#selected-title').textContent=mfName(run);$('#selected-subtitle').textContent=[mfValue(run.model),mfValue(run.task),t('Seed')+' '+mfValue(run.seed),mfValue(run.campaign_id)].join(' · ');
  $('#run-source').textContent=mfSource(run);$('#run-source').className='pill real';$('#run-state').textContent=t(run.status||'Unknown')+' · '+t(run.phase||'Unknown');$('#run-state').className='status '+(run.status||'unknown');
  $('#run-observed').textContent=`${t(run.stale?'Worker stale':'Last emitted')}: ${run.last_emitted_at||t('Original time unavailable')} · ${t('Observed')}: ${run.observed_at||t('Unknown')}`;
  $('.data-badge').textContent=mfSource(run);$('#train-loss').textContent=fmt(run.train_loss);$('#eval-loss').textContent=t('Not run');$('#learning-rate').textContent=mfNumber(last.lr_used)?last.lr_used.toExponential(2):'—';
  $('#current-step').textContent=`${mfValue(run.optimizer_step)} / ${mfValue(run.total_updates)}`;$('#chart-context').textContent=`${steps.length} ${t('retained points')} · ${mfSource(run)} · ${t('Raw loss · no smoothing')}`;
  $('#loss-chart').innerHTML=mfChart(steps,'train_loss','Train loss','optimizer_step');
  $('#run-controls').innerHTML=`<span class="field-note">${t('Execution controls unavailable')}</span>`+['start','pause','resume','cancel'].map(action=>`<button disabled class="button secondary" title="${escapeHtml(t('Execution controls unavailable'))}">${escapeHtml(t(action))}</button>`).join('');
  $('#run-error').hidden=!entry.errors.length && !status.telemetry_error;$('#run-error').textContent=[status.telemetry_error,...entry.errors.map(error=>error.stream+': '+error.message)].filter(Boolean).join(' · ');
  const durations=steps.map(step=>step.step_wall_seconds).filter(mfNumber),remaining=mfNumber(run.total_updates)&&mfNumber(run.optimizer_step)?Math.max(0,run.total_updates-run.optimizer_step):null;
  const eta=run.status==='running' && !run.historical && remaining!==null && durations.length?`${fmt(durations.slice(-20).reduce((a,b)=>a+b,0)/Math.min(20,durations.length)*remaining,0)} s (${t('Estimate from measured updates')})`:t('Unavailable');
  $('#inspector-config').innerHTML=mfRows([['Samples',`${mfValue(run.samples_processed)} / ${mfValue(run.samples_total)}`],['Observed span seconds',steps.length>1&&steps[0].emitted_at&&last.emitted_at?Math.max(0,(Date.parse(last.emitted_at)-Date.parse(steps[0].emitted_at))/1000):null],['Gradient before clipping',mfNumber(last.gradient_norm_before_clip)?fmt(last.gradient_norm_before_clip):mfReason(last,'gradient_norm_before_clip')],['LR used',mfNumber(last.lr_used)?last.lr_used:mfReason(last,'lr_used')],['LR next',last.lr_next],['ETA estimate',eta],['Inclusive wall seconds',summary.wall_seconds],['Wall time scope',summary.wall_time_scope],['Training succeeded',mfBool(run.training_succeeded)],['Checkpoint saved',mfBool((detail.artifacts||[]).length?(detail.artifacts||[]).some(a=>a.exists===true):null)],['Fresh reload succeeded',mfBool(run.reload_succeeded)],['Evaluation completed',mfBool(run.evaluation_completed)],['Telemetry complete',mfBool(run.telemetry_complete)],['Reconciliation',summary.reconciliation?.status],['Attempt',run.attempt_id],['Data hash',run.data_hash],['Recipe hash',run.scientific_recipe_hash],['Code SHA',run.code_sha]]).replace(/^<dl[^>]*>|<\/dl>$/g,'');
  $('#run-export-json').href=`/api/mediflow/runs/${encodeURIComponent(run.id)}/export?format=json`;$('#run-export-csv').href=`/api/mediflow/runs/${encodeURIComponent(run.id)}/export?format=csv`;
  $('#run-detail').classList.toggle('inspector-hidden',inspectorHidden);$('#inspector-toggle').textContent=t(inspectorHidden?'Show inspector':'Hide inspector');$('#inspector-toggle').setAttribute('aria-expanded',String(!inspectorHidden));
  $('#gpu-telemetry-label').textContent=t('Resource sensor samples');$('#gpu-mode').textContent=t(resource.tool_status||'Unavailable');$('#gpu-model-label').textContent=resource.gpu_uuid||t('GPU identity unavailable');$('#gpu-memory-note').textContent=t(resource.attribution||'unavailable');$('#gpu-linked').textContent=mfValue(resource.process_id);
  $('#gpu-util').textContent=mfMetric(resource,'gpu_utilization_percent',1);$('#gpu-util-bar').style.width=`${pct(resource.gpu_utilization_percent)}%`;$('#gpu-memory').textContent=mfNumber(resource.gpu_memory_used_bytes)?`${fmt(resource.gpu_memory_used_bytes/1024**3,2)} / ${mfNumber(resource.gpu_memory_total_bytes)?fmt(resource.gpu_memory_total_bytes/1024**3,2):'—'} GiB`:mfReason(resource,'gpu_memory_used_bytes');$('#gpu-memory-bar').style.width=`${mfNumber(resource.gpu_memory_used_bytes)&&mfNumber(resource.gpu_memory_total_bytes)?pct(resource.gpu_memory_used_bytes/resource.gpu_memory_total_bytes*100):0}%`;$('#gpu-temp').textContent=mfMetric(resource,'gpu_temperature_celsius',1);$('#gpu-disclaimer').textContent=t('Device readings may be shared. Process RSS and CUDA allocator are separate.');
  $('#log-count').textContent=entry.streams.events.items.length;
  if(currentTab==='resources')$('#tab-content').innerHTML=renderMediflowResources(resources,last,summary);
  else if(currentTab==='evaluation')$('#tab-content').innerHTML=renderMediflowEvaluations(entry.evaluations);
  else if(currentTab==='checkpoints')$('#tab-content').innerHTML=`<h4>${t('Artifact evidence')}</h4>${(detail.artifacts||[]).map(a=>`<div class="feature-box"><strong>${escapeHtml(a.artifact_id)}</strong>${mfRows([['Kind',a.kind],['Selection',a.selection],['Exists',mfBool(a.exists)],['Verified',mfBool(a.verified)],['Fresh reload succeeded',mfBool(a.reload_verified)],['SHA256',a.sha256]])}</div>`).join('')||`<div class="empty">${t('No checkpoint evidence')}</div>`}`;
  else if(currentTab==='configuration')$('#tab-content').innerHTML=mfRows(Object.entries(detail.manifest||run).filter(([,value])=>value===null||typeof value!=='object'));
  else $('#tab-content').innerHTML=`<div class="log-view" aria-label="${t('Durable events')}">${entry.streams.events.items.map(event=>`<div class="log-line"><span class="log-time">${mfTime(event.emitted_at)}</span><span>${escapeHtml(event.event)}</span><span class="log-message">${escapeHtml(event.message||'')} · ${escapeHtml(event.attempt_id||run.attempt_id)} · #${escapeHtml(event.seq)} · ${escapeHtml(event.provenance||'')}</span></div>`).join('')||t('No durable events yet')}</div>`;
  fitChartTypography();
}
function renderMediflowResources(resources,last,summary){
  const resource=resources.at(-1)||{};
  const series=[['gpu_utilization_percent','Device GPU utilization'],['gpu_memory_used_bytes','Device GPU memory bytes'],['host_ram_used_bytes','Host RAM bytes'],['process_rss_bytes','Process RSS bytes']];
  return `<div class="mf-resource-grid">${[['lr_used','LR used'],['gradient_norm_before_clip','Gradient before clipping']].map(([key,label])=>`<section><h4>${t(label)}</h4><div class="chart mf-chart">${mfChart(mfSteps(selectedId),key,label,'optimizer_step')}</div></section>`).join('')}</div><p class="field-note">${t('Sensor sequence axis · missing samples remain gaps. Historical series are never invented.')}</p><div class="mf-resource-grid">${series.map(([key,label])=>`<section><h4>${t(label)}</h4><p>${mfMetric(resource,key,1)}</p><div class="chart mf-chart">${mfChart(resources,key,label)}</div></section>`).join('')}</div>${mfRows([['Tool status',resource.tool_status],['Attribution',resource.attribution],['Sample gap seconds',resource.sample_gap_seconds],['CPU percent',resource.cpu_percent],['Host total RAM bytes',resource.host_ram_total_bytes],['GPU power watts',resource.gpu_power_watts],['GPU SM clock MHz',resource.gpu_sm_clock_mhz],['GPU memory clock MHz',resource.gpu_memory_clock_mhz],['CUDA allocator allocated bytes',mfNumber(last.cuda_allocated_bytes)?last.cuda_allocated_bytes:mfReason(last,'cuda_allocated_bytes')],['CUDA allocator reserved bytes',mfNumber(last.cuda_reserved_bytes)?last.cuda_reserved_bytes:mfReason(last,'cuda_reserved_bytes')],['Historical peak allocated bytes',summary.peak_allocated_bytes]])}`;
}
function mfComparability(a,b){
  const keys=['data_hash','protocol','protocol_version','precision','model','task','cohort_id','split'];
  for(const key of ['image_condition','renderer_version'])if(mfKnown(a?.[key])||mfKnown(b?.[key]))keys.push(key);
  if(keys.some(key=>!mfKnown(a?.[key])||!mfKnown(b?.[key])))return 'Comparison identity unknown';
  return keys.every(key=>a[key]===b[key])?'Matching evaluation cohort':'Different evaluation cohort';
}
function renderMediflowEvaluations(evaluations){
  if(!evaluations.length)return `<div class="empty">${t('Evaluation not run')}</div>`;
  return evaluations.map(e=>`<section class="mf-evaluation"><h4>${escapeHtml(t(e.variant==='base'?'Base':e.variant==='tuned'?'Tuned':'Unknown'))} · ${escapeHtml(mfValue(e.dataset_id))}</h4><p>${escapeHtml(t(e.status==='final'&&e.receipt_verified!==true?'Final aggregate · receipt not verified':e.status))} · ${escapeHtml(e.provenance||'')} · ${t('Receipt')}: ${mfBool(e.receipt_verified)}</p>${mfRows(['source_id','language','task','split','protocol','protocol_version','precision','cohort_id','model','data_hash'].map(key=>[key,e[key]]).concat([['Image condition',e.image_condition],['Renderer version',e.renderer_version]]))}<div class="runs-scroll"><table><thead><tr>${['Scheduled','Attempted','Valid','Invalid','Failed','Unattempted'].map(key=>`<th>${t(key)}</th>`).join('')}</tr></thead><tbody><tr>${['scheduled','attempted','valid','invalid','failed','unattempted'].map(key=>`<td>${escapeHtml(mfValue(e[key]))}</td>`).join('')}</tr></tbody></table></div>${mfAggregateTable(e.metrics)}${(e.subgroups||[]).map(group=>`<h5>${escapeHtml(group.label||group.name||group.subgroup||group.kind||t('Subgroup'))}</h5>${mfAggregateTable(group)}`).join('')}<small>${t('Partial results retain evaluator denominators. Final status requires receipt evidence.')}</small></section>`).join('');
}
function mfAggregateTable(metrics){return `<div class="runs-scroll"><table><tbody>${Object.entries(metrics||{}).map(([key,value])=>`<tr><th>${escapeHtml(key)}</th><td class="technical-value">${escapeHtml(value===null?t('Unknown'):typeof value==='object'?JSON.stringify(value):value)}</td></tr>`).join('')}</tbody></table></div>`;}
function renderMediflowCompare(){
  const runs=mediflow.runs,options=runs.map(run=>`<option value="${escapeHtml(run.id)}">${escapeHtml(mfName(run))}</option>`).join('');
  $('#workspace-view').innerHTML=featureHeading('compare',t('Compare only matching known evaluation cohorts. Training loss is not clinical quality.'))+`<div class="compare-selects"><label>${t('Experiment A')}<select id="compare-a">${options}</select></label><label>${t('Experiment B')}<select id="compare-b">${options}</select></label></div><div id="compare-output"></div>`;
  $('#compare-a').value=selectedId||runs[0]?.id||'';$('#compare-b').value=runs.find(run=>run.id!==$('#compare-a').value)?.id||'';
  for(const selector of ['#compare-a','#compare-b'])$(selector).onchange=()=>{updateMediflowComparison();refresh();};updateMediflowComparison();refresh();
}
function updateMediflowComparison(){
  const a=mediflow.runs.find(run=>run.id===$('#compare-a')?.value),b=mediflow.runs.find(run=>run.id===$('#compare-b')?.value);if(!a||!b){$('#compare-output').innerHTML=`<div class="empty">${t('Select two registered runs')}</div>`;return;}
  const ea=mediflow.entry(a.id).evaluations,eb=mediflow.entry(b.id).evaluations;
  const pairs=ea.flatMap(left=>eb.map(right=>({left,right,state:mfComparability(left,right)})));
  $('#compare-output').innerHTML=`<p>${t('Compare only matching known evaluation cohorts. Training loss is not clinical quality.')}</p>${pairs.map(pair=>`<p class="pill">${escapeHtml(pair.left.evaluation_id)} / ${escapeHtml(pair.right.evaluation_id)} · ${t(pair.state)}</p>`).join('')||`<p>${t('Comparison identity unknown')}</p>`}<div class="feature-grid">${[a,b].map(run=>`<section class="feature-box"><h3>${escapeHtml(mfName(run))}</h3><p>${mfSource(run)}</p><div class="chart mf-chart">${mfChart(mfSteps(run.id),'train_loss','Train loss','optimizer_step')}</div>${renderMediflowEvaluations(mediflow.entry(run.id).evaluations)}<div class="feature-actions">${mfExport(run.id)}</div></section>`).join('')}</div>`;
}
async function loadMediflowView(view){
  if(view==='compare'){renderMediflowCompare();return;}
  if(view==='datasets'){await renderMediflowDatasets();return;}
  $('#workspace-view').innerHTML=featureHeading(view,t('MediFlow · external engine'))+`<div class="feature-result"><p>${t('Execution belongs to the MediFlow supervisor. Forge displays durable observations.')}</p><p>${t('Execution controls unavailable')}</p></div>`;
}
async function renderMediflowDatasets(){
  const datasets=await mediflow.list('/api/mediflow/datasets','datasets');if(nav!=='datasets')return;
  $('#workspace-view').innerHTML=featureHeading('datasets',t('Immutable MediFlow dataset references · original splits and provenance preserved.'))+datasets.map(ds=>`<section class="feature-box"><h3>${escapeHtml(ds.name||ds.dataset_id)}</h3>${mfRows([['Dataset',ds.dataset_id],['Status',ds.status],['Reference registration',ds.registered===true?t('Registered'):ds.registered===false?t('Not registered'):t('Unknown')],['Records',ds.record_count],['Freeze',ds.freeze_id],['Data hash',ds.data_hash],['Splits',JSON.stringify(ds.split_counts)],['Sources',JSON.stringify(ds.sources)]])}<div class="feature-actions"><button class="button secondary" data-mf-register="${escapeHtml(ds.dataset_id)}" ${!remoteControlsAllowed||ds.registered===true?'disabled':''}>${t(ds.registered===true?'Registered immutable reference':'Register immutable reference')}</button><button class="button secondary" data-mf-metadata="${escapeHtml(ds.dataset_id)}">${t('Browse metadata')}</button></div></section>`).join('')+`<div id="mf-metadata"></div>`;
  if(!datasets.length)$('#workspace-view').innerHTML+=`<p class="empty">${t('No approved dataset manifests')}</p>`;
  $$('[data-mf-register]').forEach(button=>button.onclick=async()=>{button.disabled=true;try{await api('/api/mediflow/datasets/register',{manifest_id:button.dataset.mfRegister});await renderMediflowDatasets();}catch(error){toast(error.message,true);button.disabled=false;}});
  $$('[data-mf-metadata]').forEach(button=>button.onclick=()=>loadMediflowMetadata(button.dataset.mfMetadata,0));
}
async function loadMediflowMetadata(id,offset){
  try{
    const result=await api(`/api/mediflow/datasets/${encodeURIComponent(id)}/records?offset=${offset}&limit=100`);
    if(nav!=='datasets')return;
    const indexing=result.availability?.reason==='metadata_indexing_in_progress';
    $('#mf-metadata').innerHTML=`<h3>${escapeHtml(id)} · ${offset} / ${escapeHtml(mfValue(result.total))}</h3><p>${escapeHtml(result.availability?.reason||'')}</p>${mfAggregateTable({records:result.records})}<button id="mf-metadata-prev" class="button secondary" ${offset===0||indexing?'disabled':''}>${t('Previous')}</button><button id="mf-metadata-next" class="button secondary" ${result.has_more&&!indexing?'':'disabled'}>${t('Next')}</button>${indexing?`<button id="mf-metadata-retry" class="button secondary">${t('Retry')}</button>`:''}`;
    $('#mf-metadata-prev').onclick=()=>loadMediflowMetadata(id,Math.max(0,offset-100));
    $('#mf-metadata-next').onclick=()=>loadMediflowMetadata(id,offset+100);
    if(indexing)$('#mf-metadata-retry').onclick=()=>loadMediflowMetadata(id,offset);
  }catch(error){toast(error.message,true);}
}
