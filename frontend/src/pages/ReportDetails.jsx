import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

/**
 * ReportDetails.jsx
 * Enhanced detailed report viewer for a single task ID
 * - Handles semantic confidence score display
 * - Uses compact cards for matches on mobile
 * - Replaces internal MinIO URLs with public endpoints
 * - Shows categorized badges + semantic bars
 */
function ReportDetails() {
  const { taskId } = useParams();
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // ✅ Normalize screenshot URL (replace internal MinIO host)
  const getPublicScreenshotUrl = (url) => {
    if (!url) return "";
    return url.replace(/^minio:7000/, API_CONFIG.minIoBaseUrl);
  };

  // ✅ Fetch report details from backend
  const fetchReportDetails = async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(`${API_CONFIG.analyzerBaseUrl}/report/tasks/${taskId}`);
      if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);
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
  }, [taskId]);

  // ✅ Loading screen
  if (loading) {
    return (
      <div className="d-flex flex-column flex-lg-row">
        <Sidebar />
        <div className="flex-grow-1 bg-light min-vh-100">
          <Topbar />
          <div className="text-center py-5">
            <div className="spinner-border text-primary mb-3" role="status"></div>
            <p className="text-muted">Loading report details...</p>
          </div>
        </div>
      </div>
    );
  }

  // ✅ Error screen
  if (error) {
    return (
      <div className="d-flex flex-column flex-lg-row">
        <Sidebar />
        <div className="flex-grow-1 bg-light min-vh-100">
          <Topbar />
          <div className="container py-5 text-center">
            <i className="bi bi-exclamation-octagon text-danger fs-1 mb-3"></i>
            <h5 className="text-danger">{error}</h5>
            <button onClick={fetchReportDetails} className="btn btn-outline-primary mt-3">
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  // ✅ No data found
  if (!report) {
    return (
      <div className="d-flex flex-column flex-lg-row">
        <Sidebar />
        <div className="flex-grow-1 bg-light min-vh-100">
          <Topbar />
          <div className="text-center text-muted py-5">
            <i className="bi bi-exclamation-triangle fs-3 d-block mb-2"></i>
            <p>No report data found for this task.</p>
            <Link to="/report" className="btn btn-outline-secondary mt-3">
              <i className="bi bi-arrow-left me-1"></i> Back to Reports
            </Link>
          </div>
        </div>
      </div>
    );
  }

  const details = report.reports || {};
  const {
    sub_url = [],
    category = [],
    matched_keyword = [],
    snippet = [],
    source = [],
    screenshot_url = [],
    timestamp = [],
    confident_score = [],
  } = details;

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
                Task ID: <code>{taskId}</code>
              </p>
            </div>
            <Link to="/report" className="btn btn-sm btn-outline-secondary">
              <i className="bi bi-arrow-left me-1"></i> Back
            </Link>
          </div>

          {/* Summary */}
          <div className="card shadow-sm border-0 mb-4">
            <div className="card-body">
              <h5 className="fw-bold text-dark mb-2">{report.main_url}</h5>
              <div className="d-flex flex-wrap gap-4 small text-muted">
                <div>
                  <i className="bi bi-hash me-1"></i>
                  <strong>Total Hits:</strong> {report.total_hits}
                </div>
                <div>
                  <i className="bi bi-globe me-1"></i>
                  <a href={report.main_url} target="_blank" rel="noopener noreferrer">
                    Visit Main URL
                  </a>
                </div>
              </div>
            </div>
          </div>

          {/* Matches Table */}
          <div className="card shadow-sm border-0">
            <div className="card-body p-3 p-md-4">
              <h5 className="fw-semibold text-primary mb-3">Detected Matches</h5>

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
                      const screenshot = getPublicScreenshotUrl(screenshot_url[i]);
                      const conf = confident_score && confident_score[i] ? confident_score[i] : 0;

                      return (
                        <tr key={i}>
                          <td>{i + 1}</td>
                          <td className="text-break" style={{ maxWidth: "280px" }}>
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
                            <span className="badge bg-info text-dark">{category[i]}</span>
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
                              <a href={screenshot} target="_blank" rel="noopener noreferrer">
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
                            {new Date(timestamp[i] * 1000).toLocaleString()}
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
                  <i className="bi bi-info-circle me-2"></i>No detected matches for this task.
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default ReportDetails;
