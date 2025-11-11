import React from "react";

function truncate(text, n = 140) {
  if (!text) return "";
  return text.length > n ? text.slice(0, n - 1) + "…" : text;
}

export default function ProductCard({ product }) {
  const { name, risk_score, description, keywords } = product;

  const riskBadge = () => {
    if (risk_score >= 9) return "bg-danger";
    if (risk_score >= 7) return "bg-warning text-dark";
    return "bg-secondary";
  };

  return (
    <div className="card h-100 shadow-sm">
      <div className="card-body d-flex flex-column">
        <div className="d-flex justify-content-between align-items-start mb-2">
          <h6 className="card-title mb-0" style={{ fontSize: "0.95rem" }}>
            {name}
          </h6>
          <span className={`badge ${riskBadge()}`}>{risk_score}</span>
        </div>

        <p className="card-text small text-muted mb-2">
          {truncate(description, 120)}
        </p>

        <div className="mt-auto">
          <div className="d-flex flex-wrap gap-1 mb-2">
            {keywords &&
              keywords.slice(0, 5).map((k) => (
                <span
                  key={k}
                  className="badge bg-light text-muted small border"
                >
                  {k}
                </span>
              ))}
          </div>

          <div className="d-flex justify-content-between align-items-center">
            <button className="btn btn-sm btn-outline-primary">View</button>
            <small className="text-muted">Risk</small>
          </div>
        </div>
      </div>
    </div>
  );
}
