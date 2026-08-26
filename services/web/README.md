# WaitForDdaman Frontend (React + TypeScript + Vite)

따릉이 재배치 운영 콘솔 UI. [api](../api)가 Postgres에서 읽어 내려주는 스냅샷 데이터를
메인/상세 두 탭으로 보여준다.

## 구조

```
src/
├── main.tsx / App.tsx      # 진입점, 데이터 로딩, 탭 전환, capacity/pool 전역 상태
├── api.ts                  # 백엔드 REST 클라이언트 (getMeta/getMap/getBikes)
├── types.ts                 # API 응답 타입
├── styles.css               # 전체 스타일시트
├── pages/
│   ├── MainPage.tsx         # 메인 탭 — 지도 + 위험 대여소 TOP10
│   └── DetailPage.tsx       # 상세 탭 — capacity 패널 + 검색/필터 + 대여중단/수거 목록
├── components/
│   ├── TopBar.tsx / SummaryRow.tsx
│   ├── DistrictMap.tsx / DistrictStatsCard.tsx / RegionFilterBar.tsx
│   ├── CapacityPanel.tsx / Controls.tsx
│   └── BikeTable.tsx / DetailPanel.tsx
├── hooks/
│   ├── useCapacity.ts       # 구/지역/전체 capacity 상태 관리
│   └── useClassifiedPool.ts # pool을 capacity 기준 dest/source로 분류
└── utils/                    # 지역 매칭(regions), capacity 계산, 이미지 매핑, 포맷
```

## 실행 방법

먼저 [api](../api)를 `http://127.0.0.1:8000`에서 띄운 뒤:

```bash
npm install
npm run dev
```

`vite.config.ts`에 `/api` 프록시가 설정되어 있어 개발 서버(기본 5173 포트)에서 백엔드로
자동으로 요청이 전달된다.

## 빌드

```bash
npm run build   # tsc -b && vite build
```
