/* Shared pure calculator: browser UI and regression tests execute this same code. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.DestinationMath=api;})(typeof globalThis!=='undefined'?globalThis:this,()=>{
'use strict';
const keys=['dental','medical','local','flow'];
const defaults={w:.5,weights:{dental:45,medical:15,local:30,flow:10}};
const finite=x=>typeof x==='number'&&Number.isFinite(x);
function normalize(config){
 if(!finite(config?.w)||config.w<0||config.w>1)throw Error('목적지 반영 비중은 0~100%여야 합니다.');
 const values=keys.map(k=>config.weights?.[k]);if(values.some(v=>!finite(v)||v<0||v>100))throw Error('요소 가중치는 0~100 범위여야 합니다.');
 const sum=values.reduce((a,b)=>a+b,0);if(!sum)throw Error('의료 목적지 요소 가중치를 하나 이상 0보다 크게 설정하세요.');
 return {w:config.w,weights:Object.fromEntries(keys.map((k,i)=>[k,values[i]/sum]))};
}
function score(row,data,config,normalized=false){
 const c=normalized?config:normalize(config),g=data.groups[row.growth_id],f=data.regions[row.region_key];
 const values={dental:g?.factors.dental,medical:g?.factors.medical,local:g?.factors.local,flow:f?.status==='ok'?f.inbound_share:null};
 const missing=keys.filter(k=>c.weights[k]>0&&!finite(values[k]));
 const components=Object.fromEntries(keys.map(k=>[k,c.weights[k]===0?0:finite(values[k])?100*c.weights[k]*values[k]:null]));
 const H=!row.pending&&!missing.length?keys.reduce((sum,k)=>sum+components[k],0):null;
 const B=row.base_percentile;
 const S=row.pending||!finite(B)?null:c.w===0?B:H==null?null:(1-c.w)*B+c.w*H;
 return {node:row.node,row,B,H,S,values,components,missing,
         basePart:finite(B)?(1-c.w)*B:null,destinationPart:c.w===0?0:H==null?null:c.w*H,
         delta:S==null?null:S-B,rank:null,referenceRank:null,rankChange:null,destinationRank:null};
}
function assignRanks(items,field,target){
 const sorted=items.filter(r=>finite(r[field])).sort((a,b)=>b[field]-a[field]||a.node.localeCompare(b.node));
 let previous,rank=0;sorted.forEach((r,i)=>{if(i===0||r[field]!==previous)rank=i+1;r[target]=rank;previous=r[field];});return sorted;
}
function calculate(data,config){
 const c=normalize(config),all=data.rows.map(r=>score(r,data,c,true)),ranked=assignRanks(all,'S','rank');
 assignRanks(ranked,'B','referenceRank');const hRanked=assignRanks(all,'H','destinationRank');
 for(const r of ranked)r.rankChange=r.referenceRank-r.rank;
 return {all,ranked,byNode:new Map(all.map(r=>[r.node,r])),config:c,
         eligible:ranked.length,destinationEligible:hRanked.length,held:all.filter(r=>!r.row.pending&&r.S==null).length,pending:all.filter(r=>r.row.pending).length};
}
function readSaved(storage){try{const c=JSON.parse(storage.getItem('medical-destination-scenario-v1'));normalize(c);return c;}catch(e){return JSON.parse(JSON.stringify(defaults));}}
function scenarioQuery(config,node){const p=new URLSearchParams({w:String(config.w),wd:String(config.weights.dental),wm:String(config.weights.medical),wl:String(config.weights.local),wf:String(config.weights.flow)});if(node)p.set('node',node);return p;}
return {keys,defaults,normalize,score,calculate,readSaved,scenarioQuery};
});
