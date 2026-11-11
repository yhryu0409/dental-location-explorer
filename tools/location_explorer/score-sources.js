(async function showScoreSources() {
  const block = document.getElementById('scoreSources');
  const text = document.getElementById('scoreSourceText');
  const number = value => value == null ? '미확인' : value.toLocaleString('ko-KR') + '명';
  try {
    const response = await fetch('/api/score-sources');
    if (!response.ok) throw new Error();
    const {population_month: month, baseline, latest} = await response.json();
    const title = block.querySelector('summary');
    title.textContent = '순위에 반영된 자료 · 주민등록 ' + month.slice(0, 4) + '년 ' +
      Number(month.slice(4)) + '월 · 의료기관 ' + baseline.date + ' 관측';
    text.textContent = '점수 계산 당시 전국 치과병원·의원 ' + baseline.facilities.toLocaleString('ko-KR') +
      '곳, 치과의사 ' + number(baseline.doctors) + '.';
    if (latest) {
      title.textContent += ' | 최근 의료기관 관측 ' + latest.date;
      text.textContent += ' ' + latest.date + ' 수집 결과는 ' + latest.facilities.toLocaleString('ko-KR') +
        '곳, 치과의사 ' + number(latest.doctors) + '입니다. 화면의 순위는 위 점수 기준일을 따릅니다.';
    } else {
      text.textContent += ' 최근 수집분의 검증 정보를 확인할 수 없습니다.';
    }
  } catch {
    text.textContent = '자료 기준을 불러오지 못했습니다.';
  }
})();
