# WaitForDdaman Frontend (React + TypeScript + Vite)

따릉이 재배치 운영 콘솔 UI. `softeer/project/prototype/index.html`(단일 HTML + vanilla JS
프로토타입)을 [backend](../backend)의 FastAPI API를 사용하는 React 앱으로 옮긴 것입니다.

## 구조

```
src/
├── main.tsx / App.tsx     # 진입점, 탭 상태 및 데이터 로딩
├── api.ts                 # 백엔드 REST 클라이언트
├── types.ts                # API 응답 타입
├── styles.css              # 원본 프로토타입 스타일 (거의 그대로 이식)
├── utils/                  # 자전거 이미지 매핑, 포맷 유틸
└── components/
    ├── TopBar.tsx
    ├── MainMapTab.tsx       # 대여소 위험도 히트맵 + TOP 10
    ├── DetailTab.tsx        # 수거 우선순위 콘솔 (검색/필터/이동/확정)
    ├── BikeTable.tsx / DetailPanel.tsx / KpiRow.tsx / CapacityCard.tsx / Controls.tsx
    └── Toast.tsx
```

## 실행 방법

먼저 [backend](../backend)를 `http://127.0.0.1:8000`에서 띄운 뒤:

```bash
npm install
npm run dev
```

`vite.config.ts`에 `/api` 프록시가 설정되어 있어 개발 서버(기본 5173 포트)에서 백엔드로
자동으로 요청이 전달됩니다.

## 빌드

```bash
npm run build   # tsc -b && vite build
```
