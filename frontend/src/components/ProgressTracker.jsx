import React, {
  useEffect,
  useState,
  useRef,
  useCallback,
  useMemo,
} from "react";
import { WebSocketManager, API_CONFIG } from "../utils/apiConfig";

/**
 * ProgressTracker - Optimized Real-time Ingest Progress Component
 * - WebSocket with auto-reconnect
 * - HTTP polling fallback
 * - Memoized sub-components
 * - Efficient re-renders
 */

// Memoized stat card component
const StatCard = React.memo(({ label, value, color = "secondary" }) => (
  <div className="col-6 col-md-3">
    <div className="bg-light p-2 rounded text-center">
      <div className="small text-muted">{label}</div>
      <div className={`fw-bold fs-5 text-${color}`}>{value}</div>
    </div>
  </div>
));
StatCard.displayName = "StatCard";

// Memoized category badge list
const CategoryBadges = React.memo(({ categories = {} }) => {
  const entries = useMemo(
    () => Object.entries(categories).slice(0, 10),
    [categories]
  );

  if (entries.length === 0) return null;

  return (
    <div className="mb-3">
      <h6 className="small fw-semibold text-muted mb-2">Categories Found</h6>
      <div className="d-flex flex-wrap gap-1">
        {entries.map(([cat, count]) => (
          <span key={cat} className="badge bg-info text-dark">
            {cat}: {count}
          </span>
        ))}
      </div>
    </div>
  );
});
CategoryBadges.displayName = "CategoryBadges";

// Memoized keywords list
const KeywordBadges = React.memo(({ keywords = {} }) => {
  const entries = useMemo(
    () => Object.entries(keywords).slice(0, 8),
    [keywords]
  );

  if (entries.length === 0) return null;

  return (
    <div className="mb-3">
      <h6 className="small fw-semibold text-muted mb-2">Top Keywords</h6>
      <div className="d-flex flex-wrap gap-1">
        {entries.map(([kw, count]) => (
          <span key={kw} className="badge bg-secondary">
            {kw} ({count})
          </span>
        ))}
      </div>
    </div>
  );
});
KeywordBadges.displayName = "KeywordBadges";

// Memoized UPI samples
const UPISamples = React.memo(({ samples = [] }) => {
  if (samples.length === 0) return null;

  return (
    <div className="mb-3">
      <h6 className="small fw-semibold text-muted mb-2">UPI Samples</h6>
      <div
        className="small bg-light p-2 rounded text-monospace"
        style={{ maxHeight: "80px", overflowY: "auto", fontSize: "0.85em" }}
      >
        {samples.slice(0, 5).map((h, i) => (
          <div key={i}>{h}</div>
        ))}
      </div>
    </div>
  );
});
UPISamples.displayName = "UPISamples";

/**
 * Main ProgressTracker Component
 */
function ProgressTracker({ sessionId, apiBaseUrl = null }) {
  const [progress, setProgress] = useState(null);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState("");
  const wsRef = useRef(null);
  const pollTimeoutRef = useRef(null);
  const mountedRef = useRef(true);

  // Memoized URLs
  const wsUrl = useMemo(
    () =>
      `${(apiBaseUrl || API_CONFIG.analyzerBaseUrl).replace(
        /^http/,
        "ws"
      )}/progress/ws/${sessionId}`,
    [sessionId, apiBaseUrl]
  );

  const statusUrl = useMemo(
    () =>
      `${
        apiBaseUrl || API_CONFIG.analyzerBaseUrl
      }/progress/status/${sessionId}`,
    [sessionId, apiBaseUrl]
  );

  // Polling fallback
  const pollStatus = useCallback(async () => {
    if (!mountedRef.current) return;

    try {
      const response = await fetch(statusUrl);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (mountedRef.current) {
        setProgress(data);
        setError("");
      }
    } catch (err) {
      if (mountedRef.current) {
        console.error("[poll] Error:", err);
        setError("Failed to fetch progress");
      }
    }

    // Schedule next poll (5s interval)
    if (mountedRef.current && !connected) {
      pollTimeoutRef.current = setTimeout(pollStatus, 5000);
    }
  }, [statusUrl, connected]);

  // WebSocket setup
  const setupWebSocket = useCallback(() => {
    if (wsRef.current) return;

    const ws = new WebSocketManager(wsUrl);

    const unsubscribe = ws.subscribe((event) => {
      if (!mountedRef.current) return;

      switch (event.type) {
        case "connected":
          setConnected(true);
          setError("");
          break;
        case "message":
          setProgress(event.data);
          break;
        case "error":
          setError("WebSocket error");
          setConnected(false);
          break;
        case "disconnected":
          setConnected(false);
          // Start polling as fallback
          pollStatus();
          break;
        default:
          break;
      }
    });

    ws.connect().catch((err) => {
      console.error("[ws] Failed to connect:", err);
      if (mountedRef.current) {
        // Fallback to polling
        setConnected(false);
        pollStatus();
      }
    });

    wsRef.current = { ws, unsubscribe };
  }, [wsUrl, pollStatus]);

  // Mount/cleanup
  useEffect(() => {
    if (!sessionId) return;

    setupWebSocket();

    return () => {
      mountedRef.current = false;
      if (pollTimeoutRef.current) clearTimeout(pollTimeoutRef.current);
      if (wsRef.current) {
        wsRef.current.unsubscribe();
        wsRef.current.ws.close();
        wsRef.current = null;
      }
    };
  }, [sessionId, setupWebSocket]);

  // Loading state
  if (!progress) {
    return (
      <div className="card shadow-sm border-0">
        <div className="card-body text-center py-4 text-muted">
          <div className="spinner-border spinner-border-sm mb-2"></div>
          <p>Waiting for progress data...</p>
          {error && <small className="text-danger">{error}</small>}
        </div>
      </div>
    );
  }

  // Compute display values
  const percent = Math.round(progress.percentage || 0);
  const isComplete =
    progress.status === "completed" || progress.status === "failed";
  const isFailed = progress.status === "failed";

  return (
    <div className="card shadow-sm border-0">
      <div className="card-body p-4">
        {/* Header */}
        <div className="d-flex justify-content-between align-items-center mb-3">
          <h5 className="mb-0">
            <i
              className={`bi ${
                isComplete ? "bi-check-circle" : "bi-arrow-repeat"
              } me-2`}
            ></i>
            Ingest Progress
          </h5>
          <span
            className={`badge ${connected ? "bg-success" : "bg-secondary"}`}
          >
            {connected ? "Live" : "Polling"}
          </span>
        </div>

        {/* Progress bar */}
        <div className="mb-3">
          <div className="d-flex justify-content-between mb-1">
            <span className="fw-semibold">Processing</span>
            <span className="fw-semibold">{percent}%</span>
          </div>
          <div className="progress" style={{ height: "24px" }}>
            <div
              className={`progress-bar ${
                isFailed ? "bg-danger" : "bg-success"
              } ${
                isComplete ? "" : "progress-bar-striped progress-bar-animated"
              }`}
              role="progressbar"
              style={{ width: `${percent}%` }}
              aria-valuenow={percent}
              aria-valuemin="0"
              aria-valuemax="100"
            >
              <span className="small fw-bold text-white">{percent}%</span>
            </div>
          </div>
        </div>

        {/* Stats grid */}
        <div className="row g-2 mb-3">
          <StatCard
            label="URLs Processed"
            value={`${progress.urls_processed || 0}/${
              progress.urls_total || 0
            }`}
          />
          <StatCard
            label="Total Matches"
            value={progress.total_matches || 0}
            color="danger"
          />
          <StatCard
            label="UPI Handles"
            value={progress.upi_count || 0}
            color="warning"
          />
          <StatCard
            label="Elapsed"
            value={`${Math.round(progress.elapsed_seconds || 0)}s`}
          />
        </div>

        {/* Categories and keywords */}
        <CategoryBadges categories={progress.categories} />
        <KeywordBadges keywords={progress.keywords_top} />
        <UPISamples samples={progress.upi_samples} />

        {/* Error message */}
        {progress.error && (
          <div className="alert alert-danger small mb-3">
            <i className="bi bi-exclamation-triangle me-1"></i>
            {progress.error}
          </div>
        )}

        {/* Status badge */}
        <div className="mt-3 text-center">
          <span
            className={`badge ${
              isFailed ? "bg-danger" : isComplete ? "bg-success" : "bg-primary"
            }`}
          >
            {progress.status?.toUpperCase() || "UNKNOWN"}
          </span>
          <small className="text-muted d-block mt-1">
            {progress.timestamp || ""}
          </small>
        </div>
      </div>
    </div>
  );
}

export default React.memo(ProgressTracker);
