// 자전거 고유번호(SPB-XXXXX) 대역별 실제 기종 사진 매핑 (원본 매핑표 기준)
export function bikeImageFor(bikeId: string): string {
  const num = parseInt(String(bikeId).replace(/\D/g, ""), 10);
  if (Number.isNaN(num)) return "/img/ddarng_2.png";
  if (num >= 1 && num <= 30000) return "/img/ddarng_1_LCD.png"; // LCD형 따릉이
  if ((num >= 40001 && num <= 46999) || (num >= 70500 && num <= 71700))
    return "/img/ddarng_1_QR.png"; // QR단말기(뉴따릉이)형
  if (num >= 80001) return "/img/ddarng_ss.png"; // 새싹(소형) 따릉이
  return "/img/ddarng_2.png"; // 그 외 QR형 세부 대역
}

export const FALLBACK_BIKE_IMG =
  "data:image/svg+xml;utf8," +
  encodeURIComponent(`
<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 300 150'>
  <rect width='300' height='150' fill='#eef1ef'/>
  <circle cx='75' cy='105' r='30' fill='none' stroke='#2f6b4f' stroke-width='6'/>
  <circle cx='210' cy='105' r='30' fill='none' stroke='#2f6b4f' stroke-width='6'/>
  <path d='M75 105 L130 60 L160 60 L210 105 M130 60 L150 105 M160 60 L145 90 L75 105' fill='none' stroke='#2f6b4f' stroke-width='6' stroke-linecap='round' stroke-linejoin='round'/>
  <rect x='118' y='45' width='26' height='16' rx='3' fill='#2f6b4f'/>
</svg>`);
