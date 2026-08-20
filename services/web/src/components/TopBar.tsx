export function TopBar({ generatedAt: _generatedAt }: { generatedAt?: string }) {
  return (
    <div className="topbar">
      <div className="brand">
        <div className="mark">
          <svg viewBox="0 0 24 24" fill="none" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
            <circle cx="5.5" cy="17.5" r="3.5" />
            <circle cx="18.5" cy="17.5" r="3.5" />
            <path d="M15 6a1 1 0 100-2 1 1 0 000 2zM12 17.5V14l-3-3 4-3 2 3h2" />
          </svg>
        </div>
        <div className="brand-text">
          <b>재배치 운영 콘솔</b>
          <span>Fleet Rebalancing Console · WaitForDdaman</span>
        </div>
      </div>
    </div>
  );
}
