"""Read-only national candidate explorer. No collector or release writes.

Run python3 tools/location_explorer/server.py --port 8877 with a compatible snapshot.
Only UI assets and explicit JSON endpoints are served, never raw source files.
"""
import os
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
import argparse
import gzip
import hashlib
import json
import math
import threading
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import numpy as np
from scipy import sparse
from pyproj import Transformer
from shapely.geometry import Point, mapping
from shapely.ops import unary_union, transform

from data_paths import ROOT

HERE = Path(__file__).resolve().parent
RUN = ROOT / 'research/refresh_20261013/pending_resolution/run'
DENSITY = ROOT / 'research/m5/local_service_features/national_core/density_rebuild/a1da5b207d02ef42'
T = Transformer.from_crs(4326, 5179, always_xy=True)
INV = Transformer.from_crs(5179, 4326, always_xy=True)

def read(p):
    return json.loads(p.read_text())

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def decay(d):
    return np.exp(-d / 750) * np.maximum(1 - (d / 3000) ** 2, 0)

class Model:
    def __init__(self):
        pointer = read(ROOT / 'research/refresh_20261013/latest.json')
        self.release = ROOT / pointer['path'] / 'result.json'
        if sha(self.release) != pointer['result_sha256']:
            raise ValueError('release_hash_mismatch')
        self.data = read(self.release)
        if self.data['pins'] != read(RUN / 'integrated_model/result.json')['pins']:
            raise ValueError('model_release_mismatch')
        self.ids = read(RUN / 'integrated_model/market_ids.json')
        self.ix = {k:i for i,k in enumerate(self.ids)}
        self.candidates = read(RUN / 'integrated_model/candidate_ids.json')
        self.cix = {k:i for i,k in enumerate(self.candidates)}
        self.W = sparse.load_npz(RUN / 'integrated_model/supply_weights.npz').tocsr()
        z = np.load(RUN / 'integrated_model/population_supply.npz')
        self.population, self.supply = z['population'], z['supply']
        self.attraction, self.keep = z['attraction'], z['keep']
        self.nfac = self.W.shape[0] - len(self.candidates)
        self.groups = read(DENSITY / 'allocation.json')['groups']
        assert self.ids == [g['id'] for g in self.groups]
        self.sites = np.load(DENSITY / 'sites.npz')['xy']
        self.anchors = np.array([self.sites[g['peak']] for g in self.groups])
        self.com = read(RUN / 'commercial/features.json')['groups']
        assert sha(RUN / 'commercial/features.json') == self.data['pins']['commercial_features']
        self.medical = self.load_medical()
        self.rows = []
        agepath = ROOT / 'research/age_dental_spending/latest.json'
        ages = {}
        if agepath.exists():
            ap = read(agepath)
            age = read(ROOT / ap['path'] / 'result.json')
            if (ROOT / age['base_release']).resolve() in (self.release.resolve(), self.release.parent.resolve()):
                ages = {r['id']:r for r in age['rows']}
        for b in self.data['bundles']:
            r = dict(b)
            w = self.entry(b)
            p, s = self.population[w.indices], self.supply[w.indices]
            reproduced = float(np.sum(p * w.data / (s + w.data)))
            if abs(reproduced - b['entry_market_population_equivalent']) > 1e-5:
                raise ValueError('candidate_score_mismatch')
            r['shock_score'] = float(np.sum(p * w.data / (s + 2*w.data)))
            r['age_score'] = ages.get(b['id'], {}).get('age_adjusted_score')
            r['district'] = ' '.join(b['label'].split()[1:2])
            r.pop('destinations', None)  # Old label vintage: rebuild from current group labels.
            r.pop('price_sensitivity', None)
            self.rows.append(r)
        self.byid = {r['id']:r for r in self.rows}
        self.baseline = next(r for r in self.rows if '명촌8길 8' in r['label'])
        self._opportunity = None
        self._opportunity_lock = threading.Lock()
        self._strategies = None
        self._strategies_lock = threading.Lock()
        self._medical_context = None
        self._medical_context_lock = threading.Lock()
        self._clusters = None
        self._clusters_lock = threading.Lock()
        self._destination = None
        self._destination_lock = threading.Lock()
        self._destination_demand = None
        self._destination_demand_lock = threading.Lock()
        self._destination_wide = None
        self._destination_wide_lock = threading.Lock()

    def destination(self):
        with self._destination_lock:
            if self._destination is None:
                from destination import build_inputs
                self._destination = build_inputs(self.clusters())
            return self._destination

    def destination_demand(self, effect, weights):
        from destination_demand import DemandModel,config
        config(effect,weights)  # Reject invalid requests before loading origin data.
        with self._destination_demand_lock:
            if self._destination_demand is None:
                self._destination_demand = DemandModel(self,self.destination())
        return self._destination_demand.evaluate(effect,weights)

    def destination_wide(self):
        with self._destination_wide_lock:
            if self._destination_wide is None:
                from destination_wide import WideDemand
                self._destination_wide = WideDemand(self,self.destination())
        return self._destination_wide

    def clusters(self):
        with self._clusters_lock:
            if self._clusters is None:
                from clusters import ClusterStore
                store = ClusterStore()
                if store.data['pins']['baseline_release_sha256'] != sha(self.release):
                    raise ValueError('cluster_baseline_mismatch')
                if store.data['pins']['strategy_pointer_sha256'] != sha(ROOT/'research/location_strategy/latest.json'):
                    raise ValueError('cluster_entry_release_mismatch')
                self._clusters = store
            return self._clusters

    @lru_cache(maxsize=32)
    def cluster_detail(self, cid):
        detail=self.clusters().detail(cid)
        row=detail['cluster']
        if 'members' in row:
            detail['geometry']={'type':'FeatureCollection','features':[
                {'type':'Feature','geometry':self.geometry(self.ix[k]),
                 'properties':{'node':k,'label':self.com[self.ix[k]]['current_label']}}
                for k in row['members']]}
        return detail

    def cluster_context(self, node, mode='local'):
        if node not in self.cix: raise KeyError(node)
        try:
            return self.clusters().candidate(node, mode)
        except (OSError, ValueError, KeyError):
            return {'status':'unavailable','reason':'클러스터 자료 검증 또는 릴리스 확인이 필요합니다. 기존 입지 점수는 유지됩니다.'}

    def medical_context(self, node):
        # Context is fetched independently; a context failure cannot change scores.
        label = self.com[self.ix[node]]['current_label']
        try:
            with self._medical_context_lock:
                if self._medical_context is None:
                    from medical_context import MedicalContext
                    self._medical_context = MedicalContext()
            return self._medical_context.lookup(label)
        except (OSError, ValueError, KeyError):
            return {'status':'unavailable','reason':'의료이용 자료 검증 또는 릴리스 확인이 필요합니다. 기존 입지 점수는 유지됩니다.'}

    def strategies(self):
        with self._strategies_lock:
            if self._strategies is None:
                from strategy import Strategies
                self._strategies = Strategies(self)
            return self._strategies

    def opportunity(self):
        with self._opportunity_lock:
            if self._opportunity is None:
                from opportunity import Opportunity
                self._opportunity = Opportunity(self)
            return self._opportunity

    def entry(self, b):
        return self.W.getrow(self.nfac + self.cix[b['representative']])

    def load_medical(self):
        raw = RUN / 'hira/records.jsonl.gz'
        assert sha(raw) == self.data['pins']['medical']
        repairs = read(RUN / 'hira/coordinate_repairs.json')
        assert sha(RUN / 'hira/coordinate_repairs.json') == self.data['pins']['medical_coordinate_repairs']
        fixes = {r['id']:r for r in repairs['rows']}
        located, repaired = [], []
        with gzip.open(raw, 'rt') as f:
            for line in f:
                r = json.loads(line)
                k = hashlib.sha256(r['ykiho'].encode()).hexdigest()[:16]
                try:
                    lon, lat = float(r['XPos']), float(r['YPos'])
                    if not(124 <= lon <= 132 and 33 <= lat <= 39):
                        raise ValueError()
                    fixed = False
                except (KeyError, TypeError, ValueError):
                    fix = fixes[k]; lon, lat = fix['longitude'], fix['latitude']; fixed = True
                vals = [r.get('dety'+x) for x in ('GdrCnt','IntnCnt','ResdntCnt','SdrCnt')]
                doctors = None if any(v is None or str(v).strip()=='' for v in vals) else sum(float(v) for v in vals)
                item = {'id':k,'name':r['yadmNm'],'address':r.get('addr',''), 'code':r['clCd'],
                        'doctors':doctors,'lon':lon,'lat':lat,'xy':T.transform(lon,lat)}
                (repaired if fixed else located).append(item)
        allrows = located + repaired
        self.facilities = [r for r in allrows if r['code'] in ('41','51')]
        assert len(self.facilities) == self.nfac
        doctors = np.array([r['doctors'] for r in self.facilities], dtype=float)
        assert np.isfinite(doctors).all()
        np.testing.assert_allclose(np.asarray(self.W[:self.nfac].T @ doctors).ravel(), self.supply, atol=1e-7)
        return [r for r in allrows if r['code'] in ('01','11','21','31','41','51','92','93')]

    @lru_cache(maxsize=64)
    def geometry(self, i):
        # Calculation footprint = union of 25m site buffers, never a convex hull.
        g = unary_union([Point(x).buffer(25, resolution=4) for x in self.sites[self.groups[i]['sites']]])
        return mapping(transform(INV.transform, g))

    @lru_cache(maxsize=8)
    def district(self, code):
        sp = RUN / f'population/support/{code}.json'
        ep = RUN / f'population/edges/{code}.npz'
        pin = self.data['source_pins'][code]
        if sha(sp) != pin['support'] or sha(ep) != pin['edges']:
            raise ValueError('population_source_hash_mismatch')
        cells = read(sp)['cells']
        z = np.load(ep); rr, cc, dd = z['rr'], z['cc'], z['dd']
        # Edges can be reused from an older population vintage. Only distances
        # and indices are reused; current resident mass comes from pinned support.
        pp = np.array([c[2] for c in cells], dtype=float)
        assert len(pp) == len(z['pop'])
        weight = decay(dd) * self.attraction[rr] * self.keep[rr]
        den = 1 + np.bincount(cc, weights=weight, minlength=len(pp))
        A = sparse.csr_matrix((pp[cc]*weight/den[cc], (cc,rr)),shape=(len(pp),len(self.ids)))
        np.testing.assert_allclose(np.array(A.sum(axis=1)).ravel()+pp/den,pp,atol=1e-6)
        return cells, A

    @lru_cache(maxsize=256)
    def district_markets(self, code):
        with np.load(RUN / f'population/edges/{code}.npz') as z:
            return set(np.unique(z['rr']).tolist())

    @lru_cache(maxsize=24)
    def detail(self, key):
        b = self.byid[key]; w = self.entry(b)
        fractions = w.data / (self.supply[w.indices] + w.data)
        parts = self.population[w.indices] * fractions
        order = np.argsort(-parts)
        dest = [{'id':self.ids[w.indices[k]],'label':self.com[w.indices[k]]['current_label'],
                 'contribution':float(parts[k]),'supply':float(self.supply[w.indices[k]]),
                 'population':float(self.population[w.indices[k]])} for k in order]
        indices = {self.ix[m] for m in b['members']}
        # Population attribution uses every source district touching any destination,
        # not a radius cut-off or the candidate's administrative region.
        contributions = []; total = 0.
        destination_indices = set(w.indices.tolist())
        for code in sorted(self.data['source_pins']):
            if not self.district_markets(code).intersection(destination_indices):
                continue
            cells, A = self.district(code)
            v = np.asarray(A[:,w.indices] @ fractions).ravel()
            total += float(v.sum())
            for j in np.flatnonzero(v>0):
                c = cells[j]
                contributions.append([float(c[0]),float(c[1]),float(c[2]),float(v[j])])
        if abs(total-b['entry_market_population_equivalent']) > 0.02:
            raise ValueError('origin_attribution_score_mismatch')
        # Mass-preserving display aggregation only; score calculation remains exact.
        aggregated = len(contributions)>8000
        if aggregated:
            bins = {}
            for lon,lat,pop,value in contributions:
                x,y=T.transform(lon,lat); keyxy=(math.floor(x/200),math.floor(y/200))
                a=bins.setdefault(keyxy,[0.,0.,0.,0.]);a[0]+=lon*pop;a[1]+=lat*pop;a[2]+=pop;a[3]+=value
            contributions=[[a[0]/a[2],a[1]/a[2],a[2],a[3]] for a in bins.values() if a[2]>0]
        gi = self.ix[b['representative']]
        cx,cy = self.anchors[gi]
        nearby = [i for i,(x,y) in enumerate(self.anchors) if (x-cx)**2+(y-cy)**2<=2500**2]
        draw = indices | set(nearby)
        features = [{'type':'Feature','geometry':self.geometry(i),'properties':{
            'id':self.ids[i],'label':self.com[i]['current_label'],'selected':i in indices,
            'retained':bool(self.keep[i]),'population':float(self.population[i]),
            'supply':float(self.supply[i])}} for i in sorted(draw)]
        dental_supply = self.W[:self.nfac,gi].toarray().ravel()
        dental = []
        for i,r in enumerate(self.facilities):
            if (r['xy'][0]-cx)**2+(r['xy'][1]-cy)**2<=6000**2 or dental_supply[i]>1e-8:
                dental.append({k:v for k,v in r.items() if k!='xy'} | {'supply_to_representative':float(dental_supply[i]*r['doctors'])})
        medical = [{k:v for k,v in r.items() if k!='xy'} for r in self.medical if r['code'] not in ('41','51') and (r['xy'][0]-cx)**2+(r['xy'][1]-cy)**2<=3000**2]
        return {'candidate':b,'destinations':dest,'origins':contributions,'origin_aggregated_200m':aggregated,
                'origin_total':total,'origin_error':total-b['entry_market_population_equivalent'],
                'boundaries':{'type':'FeatureCollection','features':features},'dental':dental,'medical':medical,
                'geometry_note':'고정 점포 좌표의 25m 버퍼 합집합(표시용 간소화). 공식 상권 경계·도로 장벽·보행권 아님.'}

    def meta(self):
        provinces = sorted({r['province'] for r in self.rows})
        return {'release':self.release.parent.name,'source_vintages':self.data['source_vintages'],
                'count':len(self.rows),'ranked':sum(not r['pending'] for r in self.rows),
                'regions':[{'name':p,'count':sum(r['province']==p for r in self.rows)} for p in provinces],
                'baseline':self.baseline,'limitations':self.data['limitations'],
                'canonical':False,'method':'전국 고정 후보·95% 의료입지 기준 / 신규 인력 총합 1 보존'}

    def neighborhood(self, keys, candidate):
        """User-selected union, not a new closed-catchment allocation model."""
        keys = sorted(set(keys))
        if not keys or len(keys)>80 or candidate not in self.byid:
            raise ValueError('invalid_neighborhood_selection')
        ii = np.array([self.ix[k] for k in keys], dtype=int)
        b = self.byid[candidate]
        shares = np.asarray(self.W[:self.nfac, ii].sum(axis=1)).ravel()
        if np.any(shares>1+1e-7):
            raise ValueError('supply_conservation_failed')
        dentists = [dict(name=r['name'], id=r['id'], doctors=r['doctors'],
                         allocated_supply=float(shares[j]*r['doctors']))
                    for j,r in enumerate(self.facilities) if shares[j]>1e-10]
        dentists.sort(key=lambda r:-r['allocated_supply'])
        pop = float(self.population[ii].sum())
        supply = float(self.supply[ii].sum())
        if abs(sum(r['allocated_supply'] for r in dentists)-supply)>1e-5:
            raise ValueError('neighborhood_supply_mismatch')
        origins=[]; source_population=0.; total=0.
        for code in sorted(self.data['source_pins']):
            if not self.district_markets(code).intersection(ii.tolist()):
                continue
            cells,A=self.district(code)
            values=np.asarray(A[:,ii].sum(axis=1)).ravel()
            for j in np.flatnonzero(values>0):
                c=cells[j]; v=float(values[j])
                if v>c[2]+1e-6:
                    raise ValueError('neighborhood_population_overallocation')
                source_population+=c[2]; total+=v
                origins.append([float(c[0]),float(c[1]),float(c[2]),v])
        if abs(total-pop)>.02:
            raise ValueError('neighborhood_population_mismatch')
        w=self.entry(b)
        parts=self.population[w.indices]*w.data/(self.supply[w.indices]+w.data)
        within=float(parts[np.isin(w.indices,ii)].sum())
        members=[{'id':self.ids[i],'label':self.com[i]['current_label'],
                  'population':float(self.population[i]),'supply':float(self.supply[i]),
                  'retained':bool(self.keep[i])} for i in ii]
        # Bound display payload, preserving both resident and allocated mass.
        aggregated=len(origins)>8000
        if aggregated:
            bins={}
            for lon,lat,p,v in origins:
                x,y=T.transform(lon,lat); a=bins.setdefault((math.floor(x/200),math.floor(y/200)),[0.,0.,0.,0.])
                a[0]+=lon*p; a[1]+=lat*p; a[2]+=p; a[3]+=v
            origins=[[a[0]/a[2],a[1]/a[2],a[2],a[3]] for a in bins.values() if a[2]>0]
        return {'members':members,'population':pop,'supply':supply,
                'source_population':source_population,'capture_share':pop/source_population if source_population else None,
                'candidate':{'id':candidate,'label':b['label'],'total_entry':b['entry_market_population_equivalent'],
                             'within_entry':within,'outside_entry':float(parts.sum())-within,'pending':b['pending']},
                'dental':dentists,'origins':origins,'aggregated':aggregated,'origin_error':total-pop,
                'boundaries':{'type':'FeatureCollection','features':[{'type':'Feature','geometry':self.geometry(i),
                              'properties':members[j]} for j,i in enumerate(ii)]}}

def serve(model, port):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            u = urlparse(self.path); q = parse_qs(u.query)
            try:
                if u.path == '/api/meta': data = model.meta()
                elif u.path == '/api/score-sources':
                    from score_sources import summary
                    data = summary(model, ROOT / 'var/m2_completion/current_model/operational_sources')
                elif u.path == '/api/region':
                    name = q.get('name',[''])[0]
                    data = [r for r in model.rows if not name or r['province']==name]
                elif u.path == '/api/candidate': data = model.detail(q.get('id',[''])[0])
                elif u.path == '/api/neighborhood':
                    data = model.neighborhood(q.get('markets',[''])[0].split(','),q.get('candidate',[''])[0])
                elif u.path == '/api/opportunity/meta': data = model.opportunity().meta()
                elif u.path == '/api/opportunity/region':
                    name=q.get('name',[''])[0]
                    data=[r for r in model.opportunity().data['rows'] if not name or r['province']==name]
                elif u.path == '/api/opportunity/detail': data = model.opportunity().detail(q.get('id',[''])[0])
                elif u.path == '/api/strategy/meta': data = model.strategies().meta()
                elif u.path == '/api/medical-context': data = model.medical_context(q.get('node',[''])[0])
                elif u.path == '/api/destination': data = model.destination()
                elif u.path in ('/api/destination/wide','/api/destination/wide/origins'):
                    from destination_wide import DEFAULTS,config
                    settings=config({k:float(q.get(k,[str(v)])[0]) for k,v in DEFAULTS.items()})
                    engine=model.destination_wide()
                    data=engine.origins(q.get('node',[''])[0],settings) if u.path.endswith('/origins') else engine.evaluate(settings)
                elif u.path == '/api/destination/demand':
                    data = model.destination_demand(float(q.get('effect',['0.5'])[0]),[float(q.get(k,[str(v)])[0]) for k,v in zip(('wd','wm','wl','wf'),(45,15,30,10))])
                elif u.path == '/api/clusters': data = model.clusters().index(q.get('mode',['local'])[0])
                elif u.path == '/api/clusters/detail': data = model.cluster_detail(q.get('id',[''])[0])
                elif u.path == '/api/cluster-context': data = model.cluster_context(q.get('node',[''])[0],q.get('mode',['local'])[0])
                elif u.path == '/api/strategy/region':
                    data = model.strategies().region(q.get('region',['울산광역시'])[0],q.get('mode',['local'])[0],
                                                    compact=q.get('view',[''])[0]=='summary')
                elif u.path == '/api/strategy/detail':
                    data = model.strategies().detail(q.get('region',['울산광역시'])[0],q.get('mode',['local'])[0],
                        q.get('id',[''])[0],q.get('node',[''])[0],int(q.get('team',['1'])[0]))
                elif u.path in ('/','/index.html','/app.js','/style.css','/opportunity.html','/opportunity.js','/strategy.html','/strategy.js','/medical-context.js','/clusters.html','/clusters.js','/clusters.css','/cluster-context.js','/destination.html','/destination.js','/destination.css','/destination-math.js','/destination-demand-math.js','/destination-wide-math.js','/destination-preview.js','/score-sources.js'):
                    path = HERE / ('index.html' if u.path=='/' else u.path[1:])
                    mime = {'.html':'text/html','.js':'text/javascript','.css':'text/css'}[path.suffix]
                    return self.send_data(path.read_bytes(), mime)
                else: return self.send_error(404)
                self.send_data(json.dumps(data,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode(),'application/json')
            except KeyError: self.send_error(404, 'Unknown candidate')
            except FileNotFoundError: self.send_error(503, 'Research comparison is not built yet')
            except ValueError: self.send_error(400, 'Invalid request or source validation failed')
            except (BrokenPipeError, ConnectionResetError):
                # A cancelled browser request is not a failed source validation.
                pass
            except Exception as exc:
                print('request_failed',type(exc).__name__,flush=True)
                self.send_error(500,'Data validation failed; inspect local sources')
        def send_data(self, payload, mime):
            self.send_response(200); self.send_header('Content-Type',mime+'; charset=utf-8')
            self.send_header('Cache-Control','no-store'); self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Length',str(len(payload))); self.end_headers();self.wfile.write(payload)
        def log_message(self,*args): pass
    print(f'Local: http://127.0.0.1:{port}',flush=True)
    ThreadingHTTPServer(('127.0.0.1',port),Handler).serve_forever()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=8877)
    args = parser.parse_args()
    if not (ROOT / 'research/refresh_20261013/latest.json').is_file():
        parser.exit(2, 'The nationwide explorer requires a local research snapshot. '
                       'See tools/location_explorer/README.md for data and demo options.\n')
    serve(Model(),args.port)
