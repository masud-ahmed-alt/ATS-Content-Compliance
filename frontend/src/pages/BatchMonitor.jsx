import { useState, useEffect, useRef } from "react";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

/**
 * BatchMonitor.jsx - Real-time Batch Processing Dashboard
 *
 * Features:
 * - Live ingest progress tracking via WebSocket
 * - Real-time category distribution
 * - Match timeline visualization
 * - Session history
 * - Failed URL retry mechanism
 */
function BatchMonitor() {
  const [sessions, setSessions] = useState({}); // { session_id: { status, progress... } }
  const [selectedSession, setSelectedSession] = useState(null);
  const [connectionStatus, setConnectionStatus] = useState("disconnected");
  const [activeSessions, setActiveSessions] = useState([]);
  const wsRef = useRef(null);

  // ✅ Connect to progress WebSocket
  useEffect(() => {
    const connectWebSocket = () => {
      if (selectedSession) {
        const wsUrl = `${API_CONFIG.analyzerBaseUrl.replace(
          "http",
          "ws"
        )}/progress/ws/${selectedSession}`;
        console.log(`[ws] Connecting to ${wsUrl}`);

        const newWs = new WebSocket(wsUrl);

        newWs.onopen = () => {
          setConnectionStatus("connected");
          console.log("[ws] Connected");
        };

        newWs.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            console.log("[ws:update]", data);
            setSessions((prev) => ({
              ...prev,
              [selectedSession]: data,
            }));
          } catch (e) {
            console.error("[ws:parse] Error:", e);
          }
        };

        newWs.onerror = (error) => {
          setConnectionStatus("error");
          console.error("[ws:error]", error);
        };

        newWs.onclose = () => {
          setConnectionStatus("disconnected");
          console.log("[ws] Disconnected");
        };

        wsRef.current = newWs;
      }
    };

    connectWebSocket();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [selectedSession]);

  // Fetch all active sessions from server
  useEffect(() => {
    let isMounted = true;
    const fetchSessions = async () => {
      try {
        const response = await fetch(`${API_CONFIG.fetcherBaseUrl}/active`);
        if (!response.ok) {
          throw new Error("Failed to load active sessions");
        }
        const data = await response.json();
        if (isMounted) {
          setActiveSessions(data.requests || []);
        }
      } catch (error) {
        console.error("Error fetching sessions:", error);
      }
    };

    fetchSessions();
    const interval = setInterval(fetchSessions, 5000);
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  useEffect(() => {
    if (!selectedSession && activeSessions.length > 0) {
      setSelectedSession(activeSessions[0].request_id);
    }
  }, [activeSessions, selectedSession]);

  const currentSession = selectedSession ? sessions[selectedSession] : null;

  const formatStartTime = (isoString) => {
    if (!isoString) return "—";
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return "—";
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  };

  // Calculate progress percentage
  const progressPercent = currentSession
    ? (currentSession.urls_processed / currentSession.urls_total) * 100 || 0
    : 0;

  // Calculate processing speed (URLs/minute)
  const processingSpeed =
    currentSession && currentSession.elapsed_seconds > 0
      ? (
        (currentSession.urls_processed / currentSession.elapsed_seconds) *
        60
      ).toFixed(2)
      : "0";

  // Estimate time remaining
  const timeRemaining =
    currentSession && parseFloat(processingSpeed) > 0
      ? Math.ceil(
        ((currentSession.urls_total - currentSession.urls_processed) /
          parseFloat(processingSpeed)) *
        60
      )
      : 0;

  return (
    <div className="d-flex flex-column flex-lg-row">
      <Sidebar />

      <div className="flex-grow-1 bg-light min-vh-100">
        <Topbar />

        <div className="container-fluid py-4 px-3 px-md-4">
          {/* Header */}
          <div className="d-flex justify-content-between align-items-center mb-4">
            <h3 className="fw-bold text-primary mb-0">
              <i className="bi bi-clock-history me-2"></i>Batch Monitor
            </h3>
            <span
              className={`badge ${connectionStatus === "connected" ? "bg-success" : "bg-danger"
                }`}
            >
              <i
                className={`bi bi-circle-fill me-1 ${connectionStatus === "connected"
                    ? "text-success"
                    : "text-danger"
                  }`}
              ></i>
              {connectionStatus === "connected" ? "Live" : "Offline"}
            </span>
          </div>

          <div className="row g-4">
            {/* Session Input */}
            <div className="col-12">
              <div className="card border-0 shadow-sm">
                <div className="card-body p-3 p-md-4">
                  <label className="form-label">
                    Enter Session ID to Monitor
                  </label>
                  <div className="input-group">
                    <input
                      type="text"
                      className="form-control"
                      placeholder="e.g., batch-1234-5678"
                      value={selectedSession || ""}
                      onChange={(e) => setSelectedSession(e.target.value)}
                    />
                    <button
                      className="btn btn-primary"
                      onClick={() => {
                        if (selectedSession) {
                          setSessions((prev) => ({
                            ...prev,
                            [selectedSession]: {
                              session_id: selectedSession,
                              status: "connecting",
                              urls_processed: 0,
                              urls_total: 0,
                              urls_failed: 0,
                              percentage: 0,
                              total_matches: 0,
                              categories: {},
                              upi_count: 0,
                              keywords_top: {},
                              elapsed_seconds: 0,
                            },
                          }));
                        }
                      }}
                    >
                      <i className="bi bi-play-fill me-1"></i>Monitor
                    </button>
                  </div>
                </div>
              </div>
            </div>

            {/* Active Sessions */}
            <div className="col-12">
              <div className="card border-0 shadow-sm">
                <div className="card-body p-3 p-md-4">
                  <div className="d-flex justify-content-between align-items-center mb-3">
                    <h5 className="mb-0">
                      <i className="bi bi-person-workspace me-2"></i>Active
                      Sessions
                    </h5>
                    <span className="badge bg-primary">
                      {activeSessions.length}
                    </span>
                  </div>
                  {activeSessions.length === 0 ? (
                    <p className="text-muted mb-0">
                      No running batches right now.
                    </p>
                  ) : (
                    <div className="list-group">
                      {activeSessions.map((session) => (
                        <button
                          key={session.request_id}
                          type="button"
                          className={`list-group-item list-group-item-action d-flex justify-content-between align-items-center ${
                            selectedSession === session.request_id
                              ? "active"
                              : ""
                          }`}
                          onClick={() => setSelectedSession(session.request_id)}
                        >
                          <div>
                            <div className="fw-semibold">
                              {session.request_id}
                            </div>
                            <small className="text-muted">
                              Started {formatStartTime(session.started_at)}
                            </small>
                          </div>
                          <span className="badge bg-secondary">
                            {session.url_count} URLs
                          </span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>

            {/* Main Progress Card */}
            {currentSession && (
              <>
                <div className="col-12">
                  <div className="card border-0 shadow-sm">
                    <div className="card-header bg-white border-bottom">
                      <h5 className="mb-0">
                        <i className="bi bi-hourglass-split me-2"></i>Progress
                      </h5>
                    </div>
                    <div className="card-body p-3 p-md-4">
                      {/* Status Badge */}
                      <div className="mb-3">
                        <span
                          className={`badge fs-6 ${currentSession.status === "completed"
                              ? "bg-success"
                              : currentSession.status === "failed"
                                ? "bg-danger"
                                : "bg-info"
                            }`}
                        >
                          {currentSession.status === "running"
                            ? "🔄 Processing"
                            : currentSession.status === "completed"
                              ? "✓ Completed"
                              : "✗ Failed"}
                        </span>
                      </div>

                      {/* Progress Bar */}
                      <div className="mb-3">
                        <div className="d-flex justify-content-between mb-2">
                          <span className="fw-semibold">
                            {currentSession.urls_processed} /{" "}
                            {currentSession.urls_total} URLs
                          </span>
                          <span className="text-muted">
                            {progressPercent.toFixed(1)}%
                          </span>
                        </div>
                        <div className="progress" style={{ height: "24px" }}>
                          <div
                            className={`progress-bar ${currentSession.status === "completed"
                                ? "bg-success"
                                : "bg-primary"
                              } progress-bar-animated`}
                            role="progressbar"
                            style={{ width: `${progressPercent}%` }}
                            aria-valuenow={progressPercent}
                            aria-valuemin="0"
                            aria-valuemax="100"
                          >
                            <small className="fw-bold text-white">
                              {progressPercent.toFixed(0)}%
                            </small>
                          </div>
                        </div>
                      </div>

                      {/* Stats Row */}
                      <div className="row g-2 mt-4">
                        <div className="col-12 col-sm-6 col-lg-3">
                          <div className="p-3 bg-light rounded border-start border-primary border-3">
                            <small className="text-muted d-block">
                              Matches Found
                            </small>
                            <h5 className="mb-0 text-primary fw-bold">
                              {currentSession.total_matches || 0}
                            </h5>
                          </div>
                        </div>
                        <div className="col-12 col-sm-6 col-lg-3">
                          <div className="p-3 bg-light rounded border-start border-info border-3">
                            <small className="text-muted d-block">
                              Processing Speed
                            </small>
                            <h5 className="mb-0 text-info fw-bold">
                              {processingSpeed} URL/min
                            </h5>
                          </div>
                        </div>
                        <div className="col-12 col-sm-6 col-lg-3">
                          <div className="p-3 bg-light rounded border-start border-success border-3">
                            <small className="text-muted d-block">
                              Time Elapsed
                            </small>
                            <h5 className="mb-0 text-success fw-bold">
                              {Math.floor(currentSession.elapsed_seconds / 60)}m{" "}
                              {currentSession.elapsed_seconds % 60}s
                            </h5>
                          </div>
                        </div>
                        <div className="col-12 col-sm-6 col-lg-3">
                          <div className="p-3 bg-light rounded border-start border-warning border-3">
                            <small className="text-muted d-block">
                              Est. Time Left
                            </small>
                            <h5 className="mb-0 text-warning fw-bold">
                              {timeRemaining}s
                            </h5>
                          </div>
                        </div>
                      </div>

                      {/* Current Batch Info */}
                      {currentSession.current_batch && (
                        <div className="mt-4 p-3 bg-primary bg-opacity-10 rounded border-start border-primary border-3">
                          <small className="fw-semibold text-primary">
                            Current Batch
                          </small>
                          <div className="mt-2">
                            <span className="badge bg-primary me-2">
                              {currentSession.current_batch.size || 0} URLs
                            </span>
                            <span className="badge bg-warning">
                              {currentSession.current_batch.matches || 0}{" "}
                              Matches
                            </span>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                {/* Categories Distribution */}
                {currentSession.categories &&
                  Object.keys(currentSession.categories).length > 0 && (
                    <div className="col-12 col-lg-6">
                      <div className="card border-0 shadow-sm">
                        <div className="card-header bg-white border-bottom">
                          <h5 className="mb-0">
                            <i className="bi bi-pie-chart me-2"></i>Categories
                            Found
                          </h5>
                        </div>
                        <div className="card-body p-3 p-md-4">
                          <div className="list-group">
                            {Object.entries(currentSession.categories)
                              .sort((a, b) => b[1] - a[1])
                              .slice(0, 8)
                              .map(([cat, count]) => (
                                <div
                                  key={cat}
                                  className="d-flex justify-content-between align-items-center p-2 border-bottom"
                                >
                                  <span>{cat}</span>
                                  <span className="badge bg-primary">
                                    {count}
                                  </span>
                                </div>
                              ))}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                {/* Top Keywords */}
                {currentSession.keywords_top &&
                  Object.keys(currentSession.keywords_top).length > 0 && (
                    <div className="col-12 col-lg-6">
                      <div className="card border-0 shadow-sm">
                        <div className="card-header bg-white border-bottom">
                          <h5 className="mb-0">
                            <i className="bi bi-tags me-2"></i>Top Keywords
                          </h5>
                        </div>
                        <div className="card-body p-3 p-md-4">
                          <div className="list-group">
                            {Object.entries(currentSession.keywords_top)
                              .sort((a, b) => b[1] - a[1])
                              .slice(0, 8)
                              .map(([kw, count]) => (
                                <div
                                  key={kw}
                                  className="d-flex justify-content-between align-items-center p-2 border-bottom"
                                >
                                  <code className="text-primary">{kw}</code>
                                  <span className="badge bg-warning">
                                    {count}
                                  </span>
                                </div>
                              ))}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                {/* UPI Handles */}
                {currentSession.upi_count > 0 && (
                  <div className="col-12">
                    <div className="card border-0 shadow-sm border-danger">
                      <div className="card-header bg-danger bg-opacity-10 border-danger">
                        <h5 className="mb-0 text-danger">
                          <i className="bi bi-exclamation-triangle-fill me-2"></i>
                          UPI Handles Detected
                        </h5>
                      </div>
                      <div className="card-body p-3 p-md-4">
                        <p className="text-muted mb-2">
                          {currentSession.upi_count} unique UPI handles found:
                        </p>
                        <div className="d-flex flex-wrap gap-2">
                          {currentSession.upi_samples &&
                            currentSession.upi_samples.map((handle, idx) => (
                              <code key={idx} className="badge bg-danger p-2">
                                {handle}
                              </code>
                            ))}
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </>
            )}

            {/* No Session Selected */}
            {!currentSession && (
              <div className="col-12">
                <div className="text-center py-5">
                  <i
                    className="bi bi-inbox text-muted"
                    style={{ fontSize: "3rem" }}
                  ></i>
                  <p className="text-muted mt-3">
                    Enter a session ID above to monitor batch progress in
                    real-time
                  </p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default BatchMonitor;
