"""Nationwide observed medical agglomerations, separate from entry scores.

Explicit build only. Bounded observation circles prevent density chains from
joining an entire city. Overlapping circles count all observed neighbours;
national coverage is deduplicated by facility ID. Municipal flows are never allocated
to clusters, and the original resident-based entry calculations are read only.
"""
import argparse
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
from scipy.spatial import cKDTree

from cluster_flows import build_flows
from medical_context import ALIASES

from data_paths import ROOT
OUT=ROOT/'research/medical_clusters'
RADIUS=750
MIN_FACILITIES=8
HOSPITAL_CODES={'01','11','21','41','92'}
ANCHOR_CODES={'01','11'}
MEDICAL_CODES={'01','11','21','31'}
DENTAL_CODES={'41','51'}
KOREAN_CODES={'92','93'}

def encode(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text())

def detect(xy,ids,codes,radius=RADIUS,minimum=MIN_FACILITIES):
    """Seed priority is frozen full-neighbourhood density, ties by stable ID.

    Every observed member is within radius of its seed. Seeds within an earlier
    circle are suppressed; overlapping circles retain ALL observed neighbours.
    The owner array is exclusive for coverage auditing, not a density measure.
    Uncovered general/tertiary hospitals become small anchors only when their
    whole neighbourhood is below the minimum.
    """
    xy=np.asarray(xy,dtype=float)
    if len(set(ids))!=len(ids) or len(ids)!=len(xy) or len(codes)!=len(ids) or not np.isfinite(xy).all():
        raise ValueError('invalid_facility_coordinates_or_ids')
    if not len(ids):return [],np.array([],dtype=int)
    tree=cKDTree(xy)
    counts=tree.query_ball_point(xy,radius,return_length=True)
    order=sorted(range(len(ids)),key=lambda i:(-int(counts[i]),ids[i]))
    owner=np.full(len(ids),-1,dtype=int);groups=[]
    for seed in order:
        if owner[seed]>=0 or counts[seed]<minimum:continue
        members=sorted(tree.query_ball_point(xy[seed],radius),key=lambda i:ids[i])
        owner[[i for i in members if owner[i]<0]]=len(groups)
        groups.append({'seed':seed,'members':members,'anchor_only':False})
    for seed in sorted(range(len(ids)),key=lambda i:ids[i]):
        if owner[seed]<0 and codes[seed] in ANCHOR_CODES:
            members=sorted(tree.query_ball_point(xy[seed],radius),key=lambda i:ids[i])
            owner[[i for i in members if owner[i]<0]]=len(groups)
            groups.append({'seed':seed,'members':members,'anchor_only':True})
    return groups,owner

def classification(counts,anchor_only):
    n=sum(counts.values())
    if anchor_only:return '독립 대형병원 거점','집적 기준 미달 병원 거점'
    if counts['01']+counts['11']:
        kind='대형병원 동반 집적'
    elif sum(counts[k] for k in DENTAL_CODES)/n>=.5:
        kind='치과 비중 높은 집적'
    elif sum(counts[k] for k in KOREAN_CODES)/n>=.5:
        kind='한의 비중 높은 집적'
    elif sum(counts[k] for k in MEDICAL_CODES)/n>=.65:
        kind='의과 비중 높은 집적'
    else:kind='의과·치과·한의 혼합'
    scale='대규모 집적(100기관 이상)' if n>=100 else '중규모 집적(30–99)' if n>=30 else '소규모 집적(8–29)'
    return kind,scale

class RegionMatcher:
    def __init__(self,regions):
        self.prefixes=[]
        for key,r in regions.items():
            names=[r['province']]+[a for a,b in ALIASES.items() if b==r['province']]
            for p in names:self.prefixes.append((p+' '+r['district'],key))
            if key=='세종특별자치시|세종시':self.prefixes.append(('세종특별자치시',key))

    def match(self,address):
        matches=[(p,key) for p,key in self.prefixes if address==p or address.startswith(p+' ')]
        if not matches:return None
        length=max(len(p) for p,key in matches)
        keys={key for p,key in matches if len(p)==length}
        return keys.pop() if len(keys)==1 else None

def entry_summary(candidates):
    valid=[r for r in candidates if not r['pending']]
    if not valid:
        return {'status':'pending' if candidates else 'no_candidate','one_median':None,
                'one_min':None,'one_max':None,'three_median':None,
                'representative_node':None,'representative_label':None}
    one=[r['one'] for r in valid];three=[r['three'] for r in valid]
    # Closest eligible existing candidate is the label, never max-score selection.
    rep=min(valid,key=lambda r:(r['distance_m'],r['node']))
    return {'status':'ok','one_median':float(np.median(one)),'one_min':min(one),'one_max':max(one),
            'three_median':float(np.median(three)),'representative_node':rep['node'],
            'representative_label':rep['label']}

def circle(center,radius,inv):
    coords=[list(inv.transform(center[0]+radius*math.cos(a),center[1]+radius*math.sin(a)))
            for a in np.linspace(0,2*math.pi,65)]
    coords[-1]=coords[0]
    return {'type':'Polygon','coordinates':[coords]}

def build(sensitivity=False):
    # Legacy detect() remains only for reproducibility/tests of retired releases.
    from commercial_clusters import build as build_commercial
    return build_commercial()

class ClusterStore:
    def __init__(self,root=OUT):
        p=read(root/'latest.json');self.release=p['release']
        if len(self.release)!=20 or any(c not in '0123456789abcdef' for c in self.release):raise ValueError('invalid_cluster_release')
        path=root/self.release/'data.json'
        if sha(path)!=p['sha256']:raise ValueError('cluster_hash_mismatch')
        self.data=read(path);self.byid={r['id']:r for r in self.data['rows']}

    def index(self,mode='local'):
        if mode not in ('local','growth'):raise ValueError('invalid_mode')
        rows=[r for r in self.data['rows'] if r.get('mode',mode)==mode]
        return {'release':self.release,'mode':mode,'summary':self.data.get('summaries',{}).get(mode,self.data['summary']),
                'definitions':self.data['definitions'],'rows':rows,'legacy_ids':self.data.get('legacy_ids',{}).get(mode,{})}

    def detail(self,cid):
        d=self.data['details'][cid]
        if 'facility_ids' not in d:return {'release':self.release,'cluster':self.byid[cid],**d}
        return {'release':self.release,'cluster':self.byid[cid],
                'facilities':[self.data['facilities'][k] for k in d['facility_ids']],
                'surrounding_facilities':[self.data['facilities'][k] for k in d['surrounding_ids']],
                'candidates':[self.data['candidates'][k] for k in d['candidate_ids']]}

    def candidate(self,node,mode='local'):
        if mode not in ('local','growth'):raise ValueError('invalid_mode')
        mapping=self.data['node_map'].get(mode,self.data['node_map'])
        cid=mapping.get(node)
        if cid is None:return {'status':'unlinked','reason':'이 후보의 기존 생활권 연결을 확인하지 못했습니다.'}
        selected=next(r for r in self.detail(cid)['candidates'] if r['node']==node)
        return {'status':'ok','release':self.release,'cluster':self.byid[cid],'candidate':selected}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--build',action='store_true');parser.add_argument('--sensitivity',action='store_true')
    args=parser.parse_args()
    if args.build:build(args.sensitivity)
