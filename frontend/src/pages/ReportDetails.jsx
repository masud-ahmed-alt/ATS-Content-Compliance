import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

function ReportDetails() {
  const { taskId } = useParams();
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(true);

  // ✅ Replace internal MinIO URL (minio:7000) with public base URL from .env
  const getPublicScreenshotUrl = (url) => {
    if (!url) return "";
    return url.replace(/^minio:7000/, API_CONFIG.minIoBaseUrl);
  };

  // ✅ Fetch report details
  const fetchReportDetails = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${API_CONFIG.analyzerBaseUrl}/report/tasks/${taskId}`);
      if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);
      const data = await response.json();
      setReport(data);
    } catch (error) {
      console.error("Error fetching report details:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchReportDetails();
  }, [taskId]);

  return (
    <div className="d-flex flex-column flex-lg-row">
      <Sidebar />

      <div className="flex-grow-1 bg-light min-vh-100">
        <Topbar />

        <div className="container-fluid py-4 px-3 px-md-4">
          {/* Header */}
          <div className="d-flex justify-content-between align-items-center mb-3">
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

          {/* Loading Spinner */}
          {loading ? (
            <div className="text-center py-5">
              <div className="spinner-border text-primary mb-3" role="status"></div>
              <p className="text-muted">Loading report details...</p>
            </div>
          ) : report ? (
            <>
              {/* Report Summary */}
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

              {/* Report Table */}
              <div className="card shadow-sm border-0">
                <div className="card-body p-3 p-md-4">
                  <h5 className="fw-semibold text-primary mb-3">Detected Matches</h5>

                  <div className="table-responsive">
                    <table className="table table-striped table-hover align-middle">
                      <thead className="table-light">
                        <tr>
                          <th scope="col">#</th>
                          <th scope="col">Sub URL</th>
                          <th scope="col">Category</th>
                          <th scope="col">Matched Keyword</th>
                          <th scope="col">Snippet</th>
                          <th scope="col">Source</th>
                          <th scope="col">Screenshot</th>
                          <th scope="col">Timestamp</th>
                        </tr>
                      </thead>
                      <tbody>
                        {report.reports.sub_url.map((url, index) => {
                          const rawScreenshotUrl = report.reports.screenshot_url
                            ? report.reports.screenshot_url[index]
                            : "";
                          const screenshotUrl = getPublicScreenshotUrl(rawScreenshotUrl);

                          return (
                            <tr key={index}>
                              <td>{index + 1}</td>
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
                              <td>
                                <span className="badge bg-info text-dark">
                                  {report.reports.category[index]}
                                </span>
                              </td>
                              <td>
                                <code>{report.reports.matched_keyword[index]}</code>
                              </td>
                              <td
                                className="small text-muted"
                                style={{
                                  whiteSpace: "pre-wrap",
                                  wordBreak: "break-word",
                                  background: "#f9fafb",
                                  borderRadius: "6px",
                                  padding: "6px",
                                  maxWidth: "600px",
                                }}
                              >
                                {report.reports.snippet[index]}
                              </td>
                              <td>
                                <span
                                  className={`badge ${
                                    report.reports.source[index] === "regex"
                                      ? "bg-success"
                                      : report.reports.source[index] === "alias"
                                      ? "bg-primary"
                                      : "bg-secondary"
                                  }`}
                                >
                                  {report.reports.source[index]}
                                </span>
                              </td>
                              <td>
                                {screenshotUrl ? (
                                  <a
                                    href={screenshotUrl}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                  >
                                    <img
                                      src={screenshotUrl}
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
                                  <span className="text-muted small">
                                    Screenshot not found
                                  </span>
                                )}
                              </td>
                              <td className="text-muted small">
                                {new Date(
                                  report.reports.timestamp[index] * 1000
                                ).toLocaleString()}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </>
          ) : (
            <div className="text-center text-muted py-5">
              <i className="bi bi-exclamation-triangle fs-3 d-block mb-2"></i>
              <p>No report data found for this task.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default ReportDetails;
