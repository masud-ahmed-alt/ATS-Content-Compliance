import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

/**
 * ReportDetails.jsx
 * Detailed report viewer for a single main URL
 * - Uses /report/tasks/{main_url}
 * - Shows Results table data (ALL matches before spaCy validation)
 * - Shows Hits table data (ONLY validated matches after spaCy)
 * - Handles spaCy confidence score display
 * - Shows categorized badges + confidence bars
 * - Replaces internal MinIO URLs with public endpoints
 */
function ReportDetails() {
  const { mainUrl } = useParams(); // param name must match App.jsx route
  const decodedMainUrl = mainUrl ? decodeURIComponent(mainUrl) : "";

  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [activeTab, setActiveTab] = useState("validated"); // "all" or "validated"

  // Normalize screenshot URL (replace internal MinIO host)
  const getPublicScreenshotUrl = (url) => {
    if (!url) return "";
    return url.replace(/^minio:7000/, API_CONFIG.minIoBaseUrl);
  };

  const fetchReportDetails = async () => {
    // guard: avoid calling backend with "undefined"
    if (!decodedMainUrl) {
      setError("Invalid URL parameter.");
      setLoading(false);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const encodedUrl = encodeURIComponent(decodedMainUrl);
      const response = await fetch(
        `${API_CONFIG.analyzerBaseUrl}/report/tasks/${encodedUrl}`
      );
      if (!response.ok)
        throw new Error(`HTTP error! Status: ${response.status}`);
      const data = await response.json();
      setReport(data);
    } catch (err) {
      console.error("Error fetching report details:", err);
      setError("Failed to load report details. Please try again later.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchReportDetails();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mainUrl]);

  // Loading
  if (loading) {
    return (
      <div className="d-flex flex-column flex-lg-row">
        <Sidebar />
        <div className="flex-grow-1 bg-light min-vh-100">
          <Topbar />
          <div className="text-center py-5">
            <div
              className="spinner-border text-primary mb-3"
              role="status"
            ></div>
            <p className="text-muted">Loading report details...</p>
          </div>
        </div>
      </div>
    );
  }

  // Error
  if (error) {
    return (
      <div className="d-flex flex-column flex-lg-row">
        <Sidebar />
        <div className="flex-grow-1 bg-light min-vh-100">
          <Topbar />
          <div className="container py-5 text-center">
            <i className="bi bi-exclamation-octagon text-danger fs-1 mb-3"></i>
            <h5 className="text-danger">{error}</h5>
            <button
              onClick={fetchReportDetails}
              className="btn btn-outline-primary mt-3"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  // No data
  if (!report || report.total_hits === undefined) {
    return (
      <div className="d-flex flex-column flex-lg-row">
        <Sidebar />
        <div className="flex-grow-1 bg-light min-vh-100">
          <Topbar />
          <div className="text-center text-muted py-5">
            <i className="bi bi-exclamation-triangle fs-3 d-block mb-2"></i>
            <p>No report data found for this URL.</p>
            <Link to="/report" className="btn btn-outline-secondary mt-3">
              <i className="bi bi-arrow-left me-1"></i> Back to Reports
            </Link>
          </div>
        </div>
      </div>
    );
  }

  // Get Results data (ALL matches before validation) and Hits data (validated matches)
  const resultsData = report.results || {};
  const hitsData = report.hits || report.reports || {}; // Fallback to reports for backward compatibility
  
  // Extract data from Hits (validated matches after spaCy)
  const {
    sub_url = [],
    category = [],
    matched_keyword = [],
    snippet = [],
    source = [],
    screenshot_url = [],
    timestamp = [],
    confident_score = [],
  } = hitsData;

  // Extract data from Results (all matches before validation)
  const allMatches = resultsData.keyword_match || [];
  const allCategories = resultsData.categories || [];
  const allSubUrls = resultsData.sub_urls || [];
  const rawData = resultsData.raw_data || "";  // All snippets from matches
  const totalAllMatches = report.total_matches_all || 0;
  const totalValidatedHits = report.total_hits || 0;
  
  // Parse snippets from raw_data (separated by ---SNIPPET---)
  const snippets = rawData ? rawData.split("---SNIPPET---").filter(s => s.trim()) : [];

  const toLocalString = (t) => {
    // supports epoch seconds or ISO strings
    if (typeof t === "number") return new Date(t * 1000).toLocaleString();
    const d = new Date(t);
    return isNaN(d.getTime()) ? String(t) : d.toLocaleString();
  };

  return (
    <div className="d-flex flex-column flex-lg-row">
      <Sidebar />

      <div className="flex-grow-1 bg-light min-vh-100">
        <Topbar />

        <div className="container-fluid py-4 px-3 px-md-4">
          {/* Header */}
          <div className="d-flex justify-content-between align-items-center mb-4">
            <div>
              <h3 className="fw-bold text-primary mb-0">Report Details</h3>
              <p className="text-muted small mb-0">
                Main URL: <code>{decodedMainUrl}</code>
              </p>
            </div>
            <Link to="/report" className="btn btn-sm btn-outline-secondary">
              <i className="bi bi-arrow-left me-1"></i> Back
            </Link>
          </div>

          {/* Status Alert */}
          {report.status && (
            <div
              className={`alert ${
                report.status === "clean"
                  ? "alert-success"
                  : report.status === "filtered"
                  ? "alert-warning"
                  : "alert-danger"
              } border-0 shadow-sm mb-4`}
              role="alert"
            >
              <div className="d-flex align-items-center">
                <div className="me-3">
                  <i
                    className={`bi ${
                      report.status === "clean"
                        ? "bi-check-circle-fill"
                        : report.status === "filtered"
                        ? "bi-filter-circle-fill"
                        : "bi-exclamation-triangle-fill"
                    } fs-3`}
                  ></i>
                </div>
                <div className="flex-grow-1">
                  <h5 className="alert-heading mb-1">{report.message}</h5>
                  <p className="mb-0 small">{report.description}</p>
                </div>
              </div>
            </div>
          )}

          {/* Summary Stats */}
          <div className="row g-3 mb-4">
            <div className="col-12 col-sm-6 col-lg-3">
              <div className="card border-0 shadow-sm">
                <div className="card-body text-center">
                  <h6 className="text-muted mb-2">All Matches</h6>
                  <h3 className="fw-bold text-info">
                    {totalAllMatches}
                  </h3>
                  <small className="text-muted">Before spaCy validation</small>
                </div>
              </div>
            </div>
            <div className="col-12 col-sm-6 col-lg-3">
              <div className="card border-0 shadow-sm">
                <div className="card-body text-center">
                  <h6 className="text-muted mb-2">Validated Hits</h6>
                  <h3 className="fw-bold text-primary">
                    {totalValidatedHits}
                  </h3>
                  <small className="text-muted">After spaCy validation</small>
                </div>
              </div>
            </div>
            <div className="col-12 col-sm-6 col-lg-3">
              <div className="card border-0 shadow-sm">
                <div className="card-body text-center">
                  <h6 className="text-muted mb-2">URLs Scanned</h6>
                  <h3 className="fw-bold text-success">
                    {allSubUrls.length}
                  </h3>
                  <small className="text-muted">Total pages analyzed</small>
                </div>
              </div>
            </div>
            <div className="col-12 col-sm-6 col-lg-3">
              <div className="card border-0 shadow-sm">
                <div className="card-body text-center">
                  <h6 className="text-muted mb-2">Categories</h6>
                  <h3 className="fw-bold text-warning">
                    {allCategories.length}
                  </h3>
                  <small className="text-muted">Categories found</small>
                </div>
              </div>
            </div>
          </div>

          {/* Tabs for All Matches vs Validated Hits */}
          <ul className="nav nav-tabs mb-3" role="tablist">
            <li className="nav-item" role="presentation">
              <button
                className={`nav-link ${activeTab === "all" ? "active" : ""}`}
                onClick={() => setActiveTab("all")}
                type="button"
              >
                <i className="bi bi-list-ul me-2"></i>
                All Matches ({totalAllMatches})
                <small className="text-muted ms-2">Before validation</small>
              </button>
            </li>
            <li className="nav-item" role="presentation">
              <button
                className={`nav-link ${activeTab === "validated" ? "active" : ""}`}
                onClick={() => setActiveTab("validated")}
                type="button"
              >
                <i className="bi bi-check-circle me-2"></i>
                Validated Hits ({totalValidatedHits})
                <small className="text-muted ms-2">After spaCy</small>
              </button>
            </li>
          </ul>

          {/* Tab Content: All Matches (Results Table) */}
          {activeTab === "all" && (
            <div className="card shadow-sm border-0 mb-4">
              <div className="card-header bg-info text-white">
                <h5 className="mb-0">
                  <i className="bi bi-database me-2"></i>
                  All Matches (Results Table - Master Data)
                </h5>
                <small>All matches found before spaCy validation</small>
              </div>
              <div className="card-body p-3 p-md-4">
                {/* Summary of All Matches */}
                <div className="row g-3 mb-4">
                  <div className="col-12 col-md-6">
                    <div className="card bg-light border-0">
                      <div className="card-body">
                        <h6 className="text-muted mb-2">All Keywords Found</h6>
                        <div className="d-flex flex-wrap gap-2">
                          {allMatches.length > 0 ? (
                            allMatches.map((kw, idx) => (
                              <span
                                key={idx}
                                className="badge bg-secondary"
                              >
                                {kw}
                              </span>
                            ))
                          ) : (
                            <span className="text-muted">No keywords found</span>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="col-12 col-md-6">
                    <div className="card bg-light border-0">
                      <div className="card-body">
                        <h6 className="text-muted mb-2">All Categories Found</h6>
                        <div className="d-flex flex-wrap gap-2">
                          {allCategories.length > 0 ? (
                            allCategories.map((cat, idx) => (
                              <span
                                key={idx}
                                className="badge bg-info text-dark"
                              >
                                {cat}
                              </span>
                            ))
                          ) : (
                            <span className="text-muted">No categories found</span>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>

                {/* All Sub URLs */}
                <div className="mb-4">
                  <h6 className="text-muted mb-3">
                    <i className="bi bi-link-45deg me-2"></i>
                    All URLs Scanned ({allSubUrls.length})
                  </h6>
                  <div className="table-responsive">
                    <table className="table table-sm table-striped">
                      <thead>
                        <tr>
                          <th>#</th>
                          <th>Sub URL</th>
                        </tr>
                      </thead>
                      <tbody>
                        {allSubUrls.map((url, idx) => (
                          <tr key={idx}>
                            <td>{idx + 1}</td>
                            <td className="text-break">
                              <a
                                href={url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-decoration-none"
                              >
                                {url}
                              </a>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Raw Data (Snippets) */}
                {snippets.length > 0 && (
                  <div className="mb-4">
                    <h6 className="text-muted mb-3">
                      <i className="bi bi-file-text me-2"></i>
                      All Snippets Found ({snippets.length})
                    </h6>
                    <div className="card border-0 bg-light">
                      <div className="card-body">
                        <div
                          className="small"
                          style={{
                            maxHeight: "400px",
                            overflowY: "auto",
                            fontFamily: "monospace",
                            fontSize: "0.85rem",
                            lineHeight: "1.6",
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                            background: "#f8f9fa",
                            padding: "12px",
                            borderRadius: "6px",
                            border: "1px solid #dee2e6",
                          }}
                        >
                          {snippets.map((snippet, idx) => (
                            <div key={idx} className="mb-3 pb-3 border-bottom">
                              <div className="d-flex justify-content-between align-items-center mb-2">
                                <span className="badge bg-secondary">Snippet {idx + 1}</span>
                                <span className="text-muted" style={{ fontSize: "0.75rem" }}>
                                  {snippet.trim().length} chars
                                </span>
                              </div>
                              <div
                                style={{
                                  background: "white",
                                  padding: "8px",
                                  borderRadius: "4px",
                                  border: "1px solid #e9ecef",
                                }}
                              >
                                {snippet.trim()}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Additional Info */}
                <div className="card bg-light border-0">
                  <div className="card-body">
                    <p className="text-muted small mb-2">
                      <strong>Task ID:</strong> {resultsData.task_id || "N/A"}
                    </p>
                    <p className="text-muted small mb-2">
                      <strong>Scan Date:</strong>{" "}
                      {resultsData.timestamp
                        ? new Date(resultsData.timestamp * 1000).toLocaleString()
                        : "N/A"}
                    </p>
                    <p className="text-muted small mb-0">
                      <i className="bi bi-info-circle me-1"></i>
                      This is the master data showing all matches found before
                      spaCy NLP validation. These matches are stored in the
                      Results table (one row per main_url). Raw data contains all
                      snippets from matches.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Tab Content: Validated Hits (Hits Table) */}
          {activeTab === "validated" && (
            <div className="card shadow-sm border-0">
              <div className="card-header bg-success text-white">
                <h5 className="mb-0">
                  <i className="bi bi-check-circle me-2"></i>
                  Validated Hits (Hits Table)
                </h5>
                <small>Only matches that passed spaCy validation</small>
              </div>
              <div className="card-body p-3 p-md-4">
                {/* Info Alert */}
                {sub_url.length > 0 && (
                  <div className="alert alert-info border-0 mb-3">
                    <i className="bi bi-info-circle me-2"></i>
                    Showing {sub_url.length} validated hit(s) out of{" "}
                    {totalAllMatches} total matches found. These hits passed
                    spaCy NLP validation.
                  </div>
                )}

                <div className="table-responsive">
                <table className="table table-striped table-hover align-middle">
                  <thead className="table-light">
                    <tr>
                      <th>#</th>
                      <th>Sub URL</th>
                      <th>Category</th>
                      <th>Matched Keyword</th>
                      <th>Snippet</th>
                      <th>Source</th>
                      <th>Confidence</th>
                      <th>Screenshot</th>
                      <th>Timestamp</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sub_url.map((url, i) => {
                      const screenshot = getPublicScreenshotUrl(
                        screenshot_url[i]
                      );
                      const conf =
                        confident_score && confident_score[i]
                          ? confident_score[i]
                          : 0;

                      return (
                        <tr key={i}>
                          <td>{i + 1}</td>
                          <td
                            className="text-break"
                            style={{ maxWidth: "280px" }}
                          >
                            <a
                              href={url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-decoration-none text-dark"
                            >
                              {url}
                            </a>
                          </td>
                          <td>
                            <span className="badge bg-info text-dark">
                              {category[i]}
                            </span>
                          </td>
                          <td>
                            <code>{matched_keyword[i]}</code>
                          </td>
                          <td
                            className="small text-muted"
                            style={{
                              whiteSpace: "pre-wrap",
                              wordBreak: "break-word",
                              background: "#f9fafb",
                              borderRadius: "6px",
                              padding: "6px",
                              maxWidth: "480px",
                            }}
                          >
                            {snippet[i]}
                          </td>
                          <td>
                            <span
                              className={`badge ${
                                source[i] === "regex"
                                  ? "bg-success"
                                  : source[i] === "alias"
                                  ? "bg-primary"
                                  : "bg-secondary"
                              }`}
                            >
                              {source[i]}
                            </span>
                          </td>

                          {/* Confidence bar */}
                          <td style={{ width: "120px" }}>
                            <div className="progress" style={{ height: "6px" }}>
                              <div
                                className={`progress-bar ${
                                  conf >= 70
                                    ? "bg-success"
                                    : conf >= 40
                                    ? "bg-warning"
                                    : "bg-danger"
                                }`}
                                role="progressbar"
                                style={{ width: `${conf}%` }}
                                aria-valuenow={conf}
                                aria-valuemin="0"
                                aria-valuemax="100"
                              ></div>
                            </div>
                            <small className="text-muted">{conf}%</small>
                          </td>

                          <td>
                            {screenshot ? (
                              <a
                                href={screenshot}
                                target="_blank"
                                rel="noopener noreferrer"
                              >
                                <img
                                  src={screenshot}
                                  alt="Screenshot"
                                  style={{
                                    width: "70px",
                                    height: "40px",
                                    objectFit: "cover",
                                    borderRadius: "4px",
                                    border: "1px solid #ddd",
                                  }}
                                />
                              </a>
                            ) : (
                              <span className="text-muted small">N/A</span>
                            )}
                          </td>
                          <td className="text-muted small">
                            {toLocalString(timestamp[i])}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* Empty state */}
              {sub_url.length === 0 && (
                <div className="text-center py-4 text-muted">
                  <i className="bi bi-info-circle me-2"></i>
                  No validated hits found. All {totalAllMatches} matches were
                  filtered out by spaCy validation.
                </div>
              )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default ReportDetails;
