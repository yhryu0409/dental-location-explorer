const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),M=require('./destination-wide-math.js');
const html=fs.readFileSync(__dirname+'/destination.html','utf8');
const fixture={release:'ui-test',meta:{reference:{candidate_count:2},vintages:{population:'test',healthcare:'test'}},groups:{g:{id:'g',label:'생활권',dental_count:2}},rows:[{node:'a',one:100,label:'가',growth_id:'g',source_region:'서울',lat:37,lon:127},{node:'b',one:90,label:'나',growth_id:'g',source_region:'서울',lat:37,lon:127}]};
async function page(search='',saved=null){
 const elements=Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(x=>[x[1],{value:'',dataset:{},innerHTML:'',textContent:'',checked:false}]));
 elements.wideOrigins={};elements.mapEvidence={};elements.sort.value='rank';elements.mapLimit.value='0';elements.mapColor.value='rank';
 const buttons=[0,25,50,75,100].map(w=>({dataset:{w},setAttribute(){}})),requests=[],store=new Map(saved?[['medical-destination-wide-v1',JSON.stringify(saved)]]:[]),location={search,pathname:'/destination.html',origin:'http://test'},history={replaceState(_,__,url){location.url=url;}};
 const context={DestinationWide:M,document:{getElementById:id=>elements[id],querySelectorAll:()=>buttons},window:{},URLSearchParams,structuredClone,AbortController,setTimeout,clearTimeout,location,history,localStorage:{getItem:k=>store.get(k)??null,setItem:(k,v)=>store.set(k,v)},fetch:async url=>{
  requests.push(url);const query=new URL(url,'http://test').searchParams,c=M.normalize(Object.fromEntries(M.keys.map(k=>[k,Number(query.get(k)??M.defaults[k])])));
  const result=url==='/api/destination'?fixture:url.includes('/origins?')?{model:'wide-choice-v1',node:query.get('node'),config:c,candidate_sources:[],hub:null}:{model:'wide-choice-v1',input_release:fixture.release,config:c,rows:fixture.rows.map(row=>{const raw=c.team===3?row.one*2.4:row.one;return {node:row.node,raw,demand:raw,parts:{local:raw,regional:0,national:0}};})};return {ok:true,json:async()=>result};
 }};
 vm.runInNewContext(fs.readFileSync(__dirname+'/destination.js','utf8'),context);
 const flush=async()=>{for(let i=0;i<5;i++)await new Promise(resolve=>setImmediate(resolve));};await flush();return {elements,buttons,requests,store,location,flush};
}
(async()=>{
 const fresh=await page();assert.equal(fresh.elements.destinationWeight.value,100);assert.equal(fresh.elements.teamSize.value,1);assert(fresh.location.url.includes('effect=1'));assert(fresh.location.url.includes('team=1'));
 const old=await page('?model=wide-choice-v1&effect=0.5&purpose=0.2&national=0.2&regionalKm=40&nationalKm=180',{...M.defaults,team:3});assert.equal(old.elements.teamSize.value,1);assert.equal(old.elements.destinationWeight.value,50);assert(old.location.url.includes('effect=0.5'));assert(old.location.url.includes('team=1'));
 old.buttons[0].onclick();await old.flush();let count=old.requests.filter(x=>x.startsWith('/api/destination/wide?')).length;
 old.elements.teamSize.onchange({target:{value:'3'}});await old.flush();assert.equal(old.requests.filter(x=>x.startsWith('/api/destination/wide?')).length,count+1);assert(old.location.url.includes('effect=0'));assert(old.location.url.includes('team=3'));assert(old.elements.rankRows.innerHTML.includes('1인당 80'));assert(old.elements.detail.innerHTML.includes('3명 팀 원점수'));assert(old.elements.weightStatus.textContent.includes('동일 3명 원점수'));
 assert.equal(JSON.parse(old.store.get('medical-destination-wide-v1')).team,3);assert(old.requests.some(x=>x.includes('/wide/origins?')&&x.includes('team=3')));
 count=old.requests.filter(x=>x.startsWith('/api/destination/wide?')).length;old.elements.teamSize.onchange({target:{value:'1'}});await old.flush();assert.equal(old.requests.filter(x=>x.startsWith('/api/destination/wide?')).length,count);
 old.elements.resetWeights.onclick();await old.flush();assert.equal(old.elements.teamSize.value,1);assert.equal(old.elements.destinationWeight.value,100);
 console.log('destination UI: fresh/reset effect 100, legacy explicit URLs, team switches, zero-effect fetch, matching baseline labels, URL/storage/origins passed');
})().catch(e=>{console.error(e);process.exitCode=1;});
