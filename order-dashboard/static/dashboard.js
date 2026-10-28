'use strict';
const $ = id => document.getElementById(id);
const colors = ['#4165e8','#16a394','#ef9a40','#af6bda','#e76d85','#57a6cf','#73864b','#ac7c59'];
const charts = {};
let chartError = null;
const number = new Intl.NumberFormat('ko-KR', {maximumFractionDigits:2});
const escapeHTML = v => String(v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let data = null, view = 'hourly', busy = false, pending = false;
let chartZoom = {}; // 자동 갱신 중 사용자가 선택한 확대 범위 유지
let chartLegend = {};
function ensureCharts() {
  try {
    if (!window.echarts) throw new Error('/static/echarts.min.js를 읽지 못했습니다. Docker 이미지에 static/echarts.min.js가 포함되어 있는지 확인하세요.');
    for (const [key, id] of Object.entries({quantity:'live-quantity',amount:'live-amount',analysis:'analysis-chart'})) {
      if (charts[key]) continue;
      const element=$(id);
      if (!element) throw new Error('그래프 영역을 찾을 수 없습니다: '+id);
      const chart=echarts.init(element);
      chart.on('datazoom', event => {const zoom = event.batch ? event.batch[0] : event; if (zoom.start !== undefined) chartZoom[key] = {start:zoom.start,end:zoom.end};});
      chart.on('legendselectchanged', event => {chartLegend[key] = event.selected;});
      charts[key]=chart;
    }
    chartError=null;
    return true;
  } catch(error) {
    chartError=error;
    return false;
  }
}
const metricLabels = {quantity:'주문량', amount:'주문액', items:'주문품목 수', delta:'주문량 증감', delta_pct:'주문량 증감률', cumulative:'누적 주문량'};
function timeLabel(value, live=false) {
  const opts = {timeZone:data?.timezone || 'Asia/Seoul', hour:'2-digit',minute:'2-digit',hour12:false};
  if (live) opts.second='2-digit'; else {opts.month='2-digit';opts.day='2-digit';}
  return new Intl.DateTimeFormat('ko-KR',opts).format(new Date(value));
}
function colorFor(name) {return colors[Math.max(0,data.customers.indexOf(name)) % colors.length];}
function lineOption(group, metric, key, live=false) {
  const zoom = chartZoom[key] || {start:0,end:100};
  return {
    backgroundColor:'transparent', animationDurationUpdate:300, color:group.series.map(s=>colorFor(s.name)),
    textStyle:{fontFamily:'Noto Sans KR, system-ui, sans-serif'},
    legend:{type:'scroll',selected:chartLegend[key]||{},top:14,left:20,right:15,icon:'roundRect',itemWidth:12,itemHeight:3,textStyle:{fontSize:10,color:'#718096'}},
    grid:{left:live?58:64,right:live?125:35,top:55,bottom:live?58:70},
    tooltip:{trigger:'axis',backgroundColor:'#fff',borderColor:'#e5eaf0',textStyle:{color:'#26364b',fontSize:12},
      formatter:params=>{
        if(!params.length) return '';
        let out=`<b>${escapeHTML(timeLabel(params[0].axisValue,live))}</b>`;
        for(const p of params){
          const val=p.value[1], series=group.series[p.seriesIndex];
          out+=`<br>${p.marker}${escapeHTML(p.seriesName)}: <b>${val===null?'—':number.format(val)}</b>${metric==='amount'?' 원':metric==='delta_pct'?'%':''}`;
          if(metric==='items' && series.products[p.dataIndex]?.length) out+=`<br><span style="color:#8793a3;font-size:10px">${escapeHTML(series.products[p.dataIndex].join(', '))}</span>`;
          if(metric==='delta' && series.delta_pct?.[p.dataIndex]!=null) out+=` <span style="color:#8793a3">(${number.format(series.delta_pct[p.dataIndex])}%)</span>`;
        }
        return out;
      }},
    xAxis:{type:'time',splitNumber:live?5:6,minInterval:live?60000:undefined,maxInterval:live?60000:undefined,axisLine:{lineStyle:{color:'#e3e8ef'}},axisTick:{show:false},axisLabel:{fontSize:10,color:'#9aa5b5',hideOverlap:true,formatter:value=>timeLabel(value,live)},splitLine:{show:false},min:live?group.window_start:group.times.length===1?group.times[0]-1800000:group.times[0],max:live?group.window_end:group.times.length===1?group.times[0]+1800000:group.times[group.times.length-1]},
    yAxis:{type:'value',minInterval:metric==='amount'||metric==='delta_pct'?0:1,axisLabel:{fontSize:10,color:'#9aa5b5',formatter:v=>Math.abs(v)>=10000?number.format(v/10000)+'만':number.format(v)},splitLine:{lineStyle:{color:'#eef1f5',type:'dashed'}}},
    dataZoom:live?[]:[{type:'inside',...zoom},{type:'slider',height:12,bottom:20,borderColor:'transparent',backgroundColor:'#f1f4f9',fillerColor:'#4165e815',handleSize:0,showDetail:false,...zoom}],
    series:group.series.map(s=>({name:s.name,type:'line',showSymbol:live||group.times.length===1,symbolSize:live?0:8,lineStyle:{width:live?2.5:2},emphasis:{focus:'series'},connectNulls:false,labelLayout:{moveOverlap:'shiftY',hideOverlap:true},step:metric==='cumulative'?'end':false,data:group.times.map((t,i)=>live&&i===group.times.length-1?{value:[t,s[metric][i]],symbolSize:7,label:{show:true,position:'right',distance:8,fontSize:10,color:colorFor(s.name),formatter:()=>`${s.name} · ${number.format(s[metric][i])}`}}:[t,s[metric][i]])})),
    graphic:live?[{type:'text',left:58,bottom:15,style:{text:'5분 전 '+timeLabel(group.window_start,true),fill:'#8b99ac',fontSize:10,fontFamily:'Noto Sans KR'}},{type:'text',right:125,bottom:15,style:{text:'현재 '+timeLabel(group.window_end,true),fill:'#4165e8',fontSize:10,fontFamily:'Noto Sans KR'}}]:group.times.length&&group.series.length?[]:[{type:'text',left:'center',top:'middle',style:{text:'표시할 주문 데이터가 없습니다',fill:'#94a0b1',fontSize:13}}]
  };
}
function hourlyOption(metric) {
  const group=data.hourly, zoom=chartZoom.analysis||{start:0,end:100};
  return {color:group.series.map(s=>colorFor(s.name)),textStyle:{fontFamily:'Noto Sans KR, system-ui, sans-serif'},
    legend:{type:'scroll',selected:chartLegend.analysis||{},top:14,left:24,right:20,itemWidth:10,itemHeight:10,textStyle:{fontSize:11,color:'#718096'}},
    grid:{left:68,right:30,top:58,bottom:75},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},backgroundColor:'#fff',borderColor:'#e5eaf0',textStyle:{color:'#26364b',fontSize:12},formatter:params=>{
      if(!params.length)return '';
      const index=params[0].dataIndex, hour=group.times[index];
      let out=`<b>${escapeHTML(timeLabel(hour))} – ${escapeHTML(timeLabel(hour+3600000))}</b>`;
      for(const p of params){out+=`<br>${p.marker}${escapeHTML(p.seriesName)}: <b>${number.format(p.value)}</b>${metric==='amount'?' 원':''}`;
        if(metric==='items'&&group.series[p.seriesIndex].products[index]?.length)out+=`<br><small>${escapeHTML(group.series[p.seriesIndex].products[index].join(', '))}</small>`;}
      return out;
    }},
    xAxis:{type:'category',data:group.times.map(t=>timeLabel(t)),axisLabel:{color:'#8b99ac',fontSize:10,hideOverlap:true},axisTick:{alignWithLabel:true},axisLine:{lineStyle:{color:'#e3e8ef'}}},
    yAxis:{type:'value',minInterval:metric==='amount'?0:1,axisLabel:{fontSize:10,color:'#9aa5b5',formatter:v=>Math.abs(v)>=10000?number.format(v/10000)+'만':number.format(v)},splitLine:{lineStyle:{color:'#eef1f5',type:'dashed'}}},
    dataZoom:[{type:'inside',...zoom},{type:'slider',height:12,bottom:20,showDetail:false,borderColor:'transparent',...zoom}],
    series:group.series.map(s=>({name:s.name,type:'bar',data:s[metric],barMaxWidth:28,barGap:'12%',barCategoryGap:'25%',itemStyle:{borderRadius:[3,3,0,0]},emphasis:{focus:'series'}})),
    graphic:group.series.some(s=>s.orders.some(v=>v>0))?[]:[{type:'text',left:'center',top:'middle',style:{text:'선택 기간에 주문이 없습니다',fill:'#94a0b1',fontSize:13}}]};
}
function productOption() {
  const zoom=chartZoom.analysis||{start:0,end:100};
  return {textStyle:{fontFamily:'Noto Sans KR, system-ui, sans-serif'},grid:{left:64,right:35,top:35,bottom:70},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>params.length?`${escapeHTML(params[0].name)}<br>총 주문량: <b>${number.format(params[0].value)}</b>`:''},
    xAxis:{type:'category',data:data.products.map(p=>p.name),axisLabel:{color:'#718096',fontSize:11,hideOverlap:true},axisTick:{show:false},axisLine:{lineStyle:{color:'#e3e8ef'}}},
    yAxis:{type:'value',minInterval:1,axisLabel:{color:'#9aa5b5',fontSize:10},splitLine:{lineStyle:{color:'#eef1f5',type:'dashed'}}},
    dataZoom:[{type:'inside',...zoom},{type:'slider',height:12,bottom:20,showDetail:false,borderColor:'transparent',...zoom}],
    series:[{type:'bar',name:'총 주문량',data:data.products.map(p=>p.quantity),barMaxWidth:46,itemStyle:{color:'#4165e8',borderRadius:[5,5,0,0]},label:{show:true,position:'top',color:'#66768b',fontSize:11,formatter:p=>number.format(p.value)}}],
    graphic:data.products.length?[]:[{type:'text',left:'center',top:'middle',style:{text:'선택 기간에 주문이 없습니다',fill:'#94a0b1',fontSize:13}}]};
}
function setMetricOptions() {
  const values=view==='trend'?['delta','quantity','delta_pct']:['quantity','amount','items'];
  $('metric').replaceChildren(...values.map(value=>{const o=document.createElement('option');o.value=value;o.textContent=metricLabels[value];return o;}));
}
function renderAnalysis() {
  if (!data || !charts.analysis) return;
  $('metric-label').hidden=view==='products'||view==='cumulative';
  $('interval-label').hidden=view==='hourly'||view==='products';
  const metric=view==='cumulative'?'cumulative':$('metric').value;
  const seconds=data.trend.seconds;
  const intervalText=seconds>=3600?`${number.format(seconds/3600)}시간`:seconds>=60?`${number.format(seconds/60)}분`:`${seconds}초`;
  const titles={hourly:`고객별 시간당 ${metricLabels[metric]}`,trend:`고객별 ${metricLabels[metric]} 추이`,cumulative:'고객별 누적 주문량',products:'제품별 총 주문량'};
  const descriptions={hourly:'시간대마다 고객별 막대를 나란히 배치해 1시간 합계를 비교합니다.',trend:`${intervalText} 구간별 주문량을 비교해 변화를 확인합니다.`,cumulative:'선택한 분석 기간의 시작부터 주문 수량을 차례로 더합니다.',products:'선택한 고객·분석 기간의 제품별 수량을 많은 순서대로 표시합니다.'};
  $('analysis-title').textContent=titles[view];$('analysis-description').textContent=descriptions[view];
  $('analysis-panel').setAttribute('aria-labelledby','tab-'+view);
  $('analysis-chart').setAttribute('aria-label',titles[view]);
  $('analysis-note').textContent=view==='trend'?'증감 = 현재 구간 수량 − 직전 구간 수량. 직전 값이 0이면 증감률은 표시하지 않습니다. 첫 구간과 현재 진행 중인 구간의 해석에 유의하세요.':view==='cumulative'?`선택 기간 내 누적값입니다. ${intervalText} 구간 끝까지 발생한 주문을 합산하며, 수정 주문은 최신 버전으로 반영됩니다.`:view==='hourly'?`각 막대는 정시부터 다음 정시 직전까지의 합계입니다. ${data.hourly.sparse?'장기간 분석에서는 주문이 있는 시간대만 표시합니다.':'주문이 없는 시간대는 0입니다.'} 현재 시간대는 진행 중인 합계입니다.`:'제품별 수량 합계입니다. 같은 주문의 수정본과 재수집 중복은 합산하지 않습니다.';
  const option=view==='products'?productOption():view==='hourly'?hourlyOption(metric):lineOption(data.trend,metric,'analysis');
  charts.analysis.setOption(option,{notMerge:true});
}
function render() {
  ensureCharts();
  const selected=$('customer').value;
  const options=['*',...data.customers];
  if(JSON.stringify([...$('customer').options].map(o=>o.value))!==JSON.stringify(options)){
    $('customer').replaceChildren(...options.map(value=>{const o=document.createElement('option');o.value=value;o.textContent=value==='*'?'전체 고객':value;return o;}));
    if(options.includes(selected)) $('customer').value=selected;
  }
  $('kpi-orders').textContent=number.format(data.summary.orders);
  $('kpi-quantity').textContent=number.format(data.summary.quantity);
  $('kpi-amount').textContent=number.format(data.summary.amount);
  $('kpi-customers').textContent=`${data.summary.customers} / ${data.summary.products}`;
  $('files').textContent=`Parquet ${number.format(data.status.file_count)}개 · 고유 주문 ${number.format(data.status.order_count)}건`;
  $('range-label').textContent=timeLabel(data.period_start)+' – '+timeLabel(data.period_end);
  const stale=data.status.last_success && Date.now()-Date.parse(data.status.last_success)>20000;
  let connection=data.status.error?'갱신 오류':!data.status.last_success?'데이터 읽는 중':stale?'갱신 지연':'실시간 연결됨';
  $('connection').className='pill '+(data.status.error?'error':!data.status.last_success||stale?'waiting':'');
  $('connection').replaceChildren(Object.assign(document.createElement('i'),{}),document.createTextNode(connection));
  $('updated').textContent=data.status.last_success?`마지막 확인 ${timeLabel(data.status.last_success,true)} · 5초 갱신`:'5초마다 자동 갱신';
  let message=data.status.error;
  if(message && data.status.last_success) message+=' 마지막 정상 데이터를 표시하고 있습니다.';
  if(!message && data.status.last_success && !data.status.order_count) message='아직 Parquet 주문 데이터가 없습니다. ETL의 저장 경로와 실행 상태를 확인하세요.';
  if(!message && data.latest_order && Date.parse(data.latest_order)>Date.now()+60000) message='주문 시각이 현재보다 미래입니다. 원천 주문 시각의 시간대와 DB_TIMEZONE 설정을 확인하세요.';
  if(!message && data.status.order_count && !data.live.series.some(s=>s.orders.some(n=>n>0))) message='직전 5분에 신규 주문이 없습니다. 주문 시뮬레이터의 실행 상태와 원본 주문 시각의 시간대를 확인하세요. 과거 데이터는 아래 분석 화면에서 볼 수 있습니다.';
  $('message').hidden=!message;$('message').textContent=message||'';
  $('live-range').textContent=`${timeLabel(data.live.window_start,true)} → 현재 ${timeLabel(data.live.window_end,true)}`;
  $('latest-order-note').textContent=data.latest_order?`최근 주문 시각: ${timeLabel(data.latest_order)} · 원본 시간대: ${data.db_timezone} · 화면 시간대: ${data.timezone}`:'최근 주문 시각: 데이터 없음';
  for(const metric of ['quantity','amount']) {
    if (charts[metric]) charts[metric].setOption(lineOption(data.live,metric,metric,true),{notMerge:true});
    const label=document.createElement('span');label.textContent='현재 구간';
    const chips=data.live.series.map(s=>{const chip=document.createElement('span'),dot=document.createElement('i'),value=document.createElement('b');dot.style.background=colorFor(s.name);value.textContent=number.format(s[metric].at(-1))+(metric==='amount'?' 원':'');chip.append(dot,document.createTextNode(s.name),value);return chip;});
    $('current-'+metric).replaceChildren(label,...chips);
  }
  renderAnalysis();
  if(chartError) window.dashboardFailure('그래프 초기화 오류', chartError.message+' 주문 데이터 조회는 계속 진행합니다.');
}
async function refresh() {
  if(busy){pending=true;return;}
  busy=true;const query=new URLSearchParams({customer:$('customer').value,live_minutes:5,period:$('period').value,interval:$('interval').value});
  const controller=new AbortController(), timeout=setTimeout(()=>controller.abort(),20000);
  let stage='API 조회';
  try {
    const response=await fetch('/api/dashboard?'+query,{cache:'no-store',signal:controller.signal});
    if(!response.ok) throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0,200)}`);
    data=await response.json();
    stage='화면 표시';
    if(data.app_version!=='2.1') throw new Error(`화면은 v2.1인데 API는 v${data.app_version||'알 수 없음'}입니다. 실행 중인 이미지와 Service 연결을 확인하세요.`);
    render();
  } catch(error) {
    const detail=error.name==='AbortError'?'20초 동안 응답이 없습니다.':error.message;
    window.dashboardFailure(stage==='API 조회'?'서버 조회 오류':'화면 표시 오류', `${stage} 실패: ${detail} 5초마다 재시도합니다.`);
  } finally {
    clearTimeout(timeout);
    busy=false;
    if(pending){pending=false;refresh();}
  }
}
for(const id of ['customer','period','interval']) $(id).addEventListener('change',()=>{chartZoom={};refresh();});
$('refresh').addEventListener('click',refresh);
$('metric').addEventListener('change',()=>{delete chartZoom.analysis;renderAnalysis();});
document.querySelectorAll('[data-view]').forEach(button=>button.addEventListener('click',()=>{
  view=button.dataset.view;delete chartZoom.analysis;
  document.querySelectorAll('[data-view]').forEach(b=>{b.classList.toggle('active',b===button);b.setAttribute('aria-selected',String(b===button));});
  setMetricOptions();renderAnalysis();
}));
const resizeCharts=()=>Object.values(charts).forEach(chart=>chart.resize());
if(window.ResizeObserver) new ResizeObserver(resizeCharts).observe(document.querySelector('main'));
else window.addEventListener('resize',resizeCharts);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
ensureCharts();
refresh(); // 웹폰트 로딩을 기다리지 않고 API를 즉시 조회합니다.
if(document.fonts) document.fonts.ready.then(resizeCharts);
setInterval(()=>{if(!document.hidden)refresh();},5000);
