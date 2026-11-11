const assert=require('assert'),M=require('./destination-wide-math.js');
const data={release:'r',rows:[{node:'a',one:100,pending:false},{node:'b',one:100,pending:false},{node:'c',one:300,pending:true}]};
for(const c of [{...M.defaults,effect:0},{...M.defaults,purpose:0}]){const r=M.calculate(data,c);assert(r.ranked.every(x=>x.demand===x.raw&&x.delta===0&&x.rankChange===0));assert.deepEqual(r.all.map(x=>x.rank),[1,1,null]);}
const response={model:'wide-choice-v1',input_release:'r',config:M.defaults,rows:[{node:'a',demand:110,parts:{local:90,regional:15,national:5}},{node:'b',demand:95,parts:{local:90,regional:4,national:1}},{node:'c',demand:null,parts:null}]};const r=M.calculate(data,M.defaults,response);assert.equal(r.byNode.get('a').delta,10);assert.equal(r.byNode.get('b').delta,-5);assert.equal(r.byNode.get('b').rankChange,-1);
for(const bad of [{...response,model:'old'},{...response,config:{...M.defaults,purpose:.3}},{...response,input_release:'old'},{...response,rows:response.rows.slice(1)},{...response,rows:response.rows.map(x=>({...x,demand:999}))}])assert.throws(()=>M.calculate(data,M.defaults,bad));
assert.throws(()=>M.normalize({...M.defaults,nationalKm:50,regionalKm:100}));assert.equal(M.scenarioQuery(M.defaults).get('model'),'wide-choice-v1');assert(!M.scenarioQuery(M.defaults).has('wd'));console.log('wide math: zero identity, path sums, ties, same cohort, version/config rejection passed');
const fixture={release:'g',rows:[{node:'a',one:100,growth_id:'g1'},{node:'b',one:90,growth_id:'g1'},{node:'c',one:100,growth_id:'g2'},{node:'d',one:20,growth_id:'g3',pending:true}]};
const base=M.calculate(fixture,{...M.defaults,effect:0}),grouped=M.groupView(base);
assert.equal(grouped.all.length,3);assert.equal(grouped.eligible,2);assert.equal(grouped.byGroup.get('g1').node,'a');assert.equal(grouped.byGroup.get('g1').demand,100);assert.deepEqual(grouped.ranked.map(x=>x.rank),[1,1]);assert(grouped.ranked.every(x=>x.rankChange===0));assert.equal(base.byNode.get('b').rank,3);
const changed={...base,all:base.all.map(r=>({...r,demand:r.node==='b'?999:r.demand}))};assert.equal(M.groupView(changed).byGroup.get('g1').node,'a');assert.equal(base.byNode.get('a').referenceRank,1);console.log('group view: unique membership, fixed representatives, no summing, ties, zero identity, no baseline mutation passed');
assert.equal(M.defaults.effect,1);assert.equal(M.defaults.team,1);
const legacy={effect:.5,purpose:.2,national:.2,regionalKm:40,nationalKm:180};
assert.deepEqual(M.normalize(legacy),{...legacy,team:1});
assert.equal(M.readSaved({getItem:()=>JSON.stringify(legacy)}).effect,.5);
assert.equal(M.readSaved({getItem:()=>JSON.stringify(legacy)}).team,1);
assert.deepEqual(M.readSaved({getItem:()=>null}),M.defaults);
assert(M.matchesConfig(legacy,{...legacy,team:1}));assert(!M.matchesConfig(legacy,{...legacy,team:3}));
assert.equal(M.scenarioQuery({...M.defaults,team:3},'a').get('team'),'3');
assert.notEqual(String(M.scenarioQuery(M.defaults)),String(M.scenarioQuery({...M.defaults,team:3})));
for(const team of [0,2,4,1.5,'3',NaN])assert.throws(()=>M.normalize({...M.defaults,team}));
const teamConfig={...M.defaults,team:3},teamResponse={model:'wide-choice-v1',input_release:'g',config:teamConfig,rows:[{node:'a',raw:220,demand:240,parts:{local:176,regional:50,national:14}},{node:'b',raw:250,demand:330,parts:{local:200,regional:100,national:30}},{node:'c',raw:280,demand:270,parts:{local:224,regional:36,national:10}},{node:'d',raw:null,demand:null,parts:null}]};
const teams=M.calculate(fixture,teamConfig,teamResponse);
assert.equal(teams.byNode.get('a').raw,220);assert.equal(teams.byNode.get('a').delta,20);assert.equal(teams.byNode.get('a').perDoctor,80);assert.equal(teams.byNode.get('a').rawPerDoctor,220/3);
assert.equal(teams.byNode.get('b').rank,1);assert.equal(teams.byNode.get('b').referenceRank,2);assert.equal(teams.byNode.get('b').rankChange,1);
assert.equal(M.groupView(teams).byGroup.get('g1').node,'a');assert.equal(M.groupView(teams).byGroup.get('g1').demand,240);
assert.equal(M.groupView(teams).byGroup.get('g1').referenceRank,2);
assert.throws(()=>M.calculate(fixture,teamConfig,{...teamResponse,config:legacy}));
assert.throws(()=>M.calculate(fixture,teamConfig,{...teamResponse,rows:teamResponse.rows.map(({raw,...x})=>x)}));
for(const config of [{...teamConfig,effect:0},{...teamConfig,purpose:0}]){
 assert(M.needsResponse(config));assert.throws(()=>M.calculate(fixture,config));
 const response={...teamResponse,config,rows:teamResponse.rows.map(r=>({...r,demand:r.raw,parts:r.raw==null?null:{local:r.raw,regional:0,national:0}}))};
 const result=M.calculate(fixture,config,response);assert(result.ranked.every(r=>r.demand===r.raw&&r.delta===0&&r.rankChange===0));
}
assert(!M.needsResponse({...M.defaults,effect:0}));
assert.equal(M.calculate(data,legacy,{...response,config:legacy}).config.team,1);
console.log('team scenarios: defaults, legacy settings, strict team config, total/per-doctor demand, matching-team baseline, zero fetch and fixed representatives passed');
