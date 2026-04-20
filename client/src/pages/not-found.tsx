export default function NotFound() {
  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="idx">404</span> Off-map
          </h1>
          <div className="page-sub">That route isn't in the nav.</div>
        </div>
      </div>
      <div
        className="panel"
        style={{ padding: 32, textAlign: "center", color: "var(--ink-3)" }}
      >
        <div className="num num--lg" style={{ marginBottom: 6 }}>
          no page at this hash
        </div>
        <div style={{ fontSize: 12 }}>
          Pick something from the sidebar.
        </div>
      </div>
    </>
  );
}
