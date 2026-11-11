/* Historical administrative-area context; no writes to score/model state. */
window.MedicalContext = (() => {
  let controller;
  const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num=(n,d=0)=>n==null?'확인 필요':Number(n).toLocaleString('ko-KR',{maximumFractionDigits:d});
  function chart(rows,field,title,unit,scale) {
    const values=rows.filter(r=>r[field]!=null).map(r=>r[field]/scale);
    if(!values.length)return `<div class="medical-chart"><h4>${title}</h4><p>완전한 관측값이 없어 추이를 표시하지 않습니다.</p></div>`;
    const max=Math.max(...values,1),width=560,height=155,step=width/rows.length;
    return `<div class="medical-chart"><h4>${title} <small>(${unit})</small></h4><svg viewBox="0 0 600 205" role="img" aria-label="${title}. 정확한 값과 보류 사유는 아래 표에 표시됩니다."><line x1="20" y1="170" x2="580" y2="170" stroke="#9aa9a4"/>${rows.map((r,i)=>{
      const v=r[field],x=20+i*step+step*.16,h=v==null?0:(v/scale/max)*height;
      return `<text x="${x+step*.34}" y="191" text-anchor="middle" font-size="11">${esc(String(r.year??r.period).slice(rows.length>5?5:0))}</text>`+(v==null?`<text x="${x+step*.34}" y="160" text-anchor="middle" font-size="10" fill="#956b3a">보류</text>`:`<rect x="${x}" y="${170-h}" width="${step*.68}" height="${h}" rx="3" fill="${field==='cost_krw'?'#217a6c':'#537a99'}"><title>${esc(r.year??r.period)}: ${num(v/scale,2)}${unit}</title></rect>`);
    }).join('')}</svg><p class="caption">최댓값 ${num(max,2)}${unit} · 빈 구간은 0이 아닙니다.</p></div>`;
  }
  function render(host,d,kind,period) {
    const g=d.region,series=g.series[kind];
    const rows=period==='annual'?series.annual:series.monthly.filter(r=>r.period.startsWith(period));
    const f=g.outflow,flow=f.status==='ok'?`<strong class="medical-share">${num(f.share*100,1)}%</strong><p>관외 ${num(f.outside_krw/1e8,2)}억 원 / 관내·관외 합계 ${num((f.inside_krw+f.outside_krw)/1e8,2)}억 원</p>`:`<p class="pending">${f.status==='zero_denominator'?'진료비 합계가 0이어서 비중을 계산하지 않습니다.':'지역 연결 또는 관내·관외 자료 확인 필요'}</p>`;
    const explain=r=>[...new Set([r.cost_status,r.claims_status].filter(x=>x!=='ok'))].map(s=>s==='incomplete'?`12개월 중 진료비 ${r.cost_krw_months}개월·건수 ${r.claims_months}개월 확인`:d.reasons[s]||'확인 필요').join(' / ')||'관측값';
    host.innerHTML=`<div class="section-head"><div><span class="eyebrow">시군구 참고 통계 · 입지 점수에 미반영</span><h2>의료이용·진료비 현황</h2><p>${esc(g.province)} ${esc(g.district)} 주민 기준</p></div></div>
      <p class="caption">${esc(d.mapping_method)}. 같은 시군구의 후보는 같은 통계를 공유합니다. 선택 생활권의 실측 수요나 병원 매출이 아닙니다.</p>
      ${g.mapping_status!=='exact_full_name'?'<p class="callout">과거 지역 구분·경계 연결을 확인 중이므로 해당 지역 수치는 보류했습니다.</p>':''}
      <div class="medical-controls"><label>의료기관 종별 <select data-medical-kind>${d.kinds.map(k=>`<option${k===kind?' selected':''}>${esc(k)}</option>`).join('')}</select></label><label>조회 기간 <select data-medical-period><option value="annual"${period==='annual'?' selected':''}>연도별 · 2019–2023</option>${[2023,2022,2021,2020,2019].map(y=>`<option value="${y}"${String(y)===period?' selected':''}>${y}년 월별</option>`).join('')}</select></label></div>
      <p>건강보험 급여 진료비 · 의료급여·비급여 제외 · 실제 관측기간 2019–2023년</p>
      <div class="panels">${chart(rows,'cost_krw','진료비 추이','억 원',1e8)}${chart(rows,'claims','진료건수 추이','만 건',1e4)}</div>
      <div class="tablewrap"><table><thead><tr><th>기간</th><th>진료비 (원)</th><th>진료건수 (건)</th><th>자료 상태</th></tr></thead><tbody>${rows.map(r=>`<tr><td>${esc(r.year??r.period)}</td><td>${num(r.cost_krw)}</td><td>${num(r.claims)}</td><td>${esc(explain(r))}</td></tr>`).join('')}</tbody></table></div>
      <p class="caption">연간 합계는 해당 지표의 12개월이 모두 확인될 때만 표시합니다. 비공개 건수는 0으로 채우지 않으며, 진료비와 건수는 각각 검증합니다. 명목 진료비 변화에는 이용량·진료 구성·수가 변화가 함께 반영됩니다.</p>
      <section class="medical-flow"><h3>전체 외래 관외 진료비 비중 · 2024년</h3>${flow}<p>지역 주민이 다른 시군구에서 이용한 진료비 ÷ 관내·관외 진료비 합계. <b>위에서 선택한 기관종별과 무관한 전체 외래 통계</b>이며, 약국 진료비·의료급여를 포함합니다. 치과·한의의 유출률이나 출발지→도착지 경로는 알 수 없습니다.</p></section>
      <p class="caption"><a href="${esc(d.sources.cost)}" target="_blank" rel="noopener">건보공단 진료비 원천</a> · <a href="${esc(f.tables?.length?'https://kosis.kr/statHtml/statHtml.do?orgId=350&tblId='+f.tables[0]:d.sources.outflow)}" target="_blank" rel="noopener">KOSIS 관내·관외 원천</a> · 한의 세부과목 분류가 불명확한 별도 파일은 연결하지 않았습니다.</p>`;
    host.querySelector('[data-medical-kind]').onchange=e=>render(host,d,e.target.value,period);
    host.querySelector('[data-medical-period]').onchange=e=>render(host,d,kind,e.target.value);
  }
  async function mount(parent,node) {
    controller?.abort();controller=new AbortController();
    const host=document.createElement('section');host.className='medical-context';parent.append(host);
    host.setAttribute('aria-label','시군구 의료이용·진료비 현황');host.innerHTML='<h2>의료이용·진료비 현황</h2><p role="status">검증된 과거 자료를 불러오는 중…</p>';
    try {
      const response=await fetch('/api/medical-context?'+new URLSearchParams({node}),{signal:controller.signal});
      if(!response.ok)throw Error('HTTP '+response.status);
      const d=await response.json();if(!host.isConnected)return;
      if(d.status!=='ok'){host.innerHTML=`<h2>의료이용·진료비 현황</h2><p class="pending">${esc(d.reason)}</p>`;return;}
      render(host,d,'치과의원','annual');
    }catch(e){if(e.name!=='AbortError'&&host.isConnected)host.innerHTML='<h2>의료이용·진료비 현황</h2><p class="pending">참고 자료를 불러오지 못했습니다. 후보를 다시 선택해 재시도할 수 있습니다. 기존 입지 점수에는 영향이 없습니다.</p>';}
  }
  return {mount};
})();
