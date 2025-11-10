import { useEffect, useState, useRef } from "react";
import { useParams } from "react-router-dom";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

function Events() {
  const { requestId } = useParams(); // /events/:requestId (optional)
  const [progressData, setProgressData] = useState({});
  const [connected, setConnected] = useState(false);
  const [retries, setRetries] = useState(0);
  const sourceRef = useRef(null);
  const reconnectTimeout = useRef(null);

  // --------- Connect SSE with auto-retry ----------
  const connectSSE = () => {
    const endpoint = requestId
      ? `${API_CONFIG.fetcherBaseUrl}/events/${requestId}`
      : `${API_CONFIG.fetcherBaseUrl}/events`;

    console.log(`[SSE] Connecting → ${endpoint}`);

    const source = new EventSource(endpoint, { withCredentials: false });
    sourceRef.current = source;

    source.onopen = () => {
      console.log("[SSE] Connected");
      setConnected(true);
      setRetries(0); // reset backoff
    };

    source.addEventListener("start", (e) => {
      try {
        const data = JSON.parse(e.data);
        setProgressData((prev) => ({
          ...prev,
          [data.url]: { ...data, percent: 0, status: "started" },
        }));
      } catch (err) {
        console.error("SSE parse error (start):", err);
      }
    });

    source.addEventListener("progress", (e) => {
      try {
        const data = JSON.parse(e.data);
        setProgressData((prev) => ({
          ...prev,
          [data.url]: {
            ...(prev[data.url] || {}),
            ...data,
            status: "processing",
          },
        }));
      } catch (err) {
        console.error("SSE parse error (progress):", err);
      }
    });

    source.addEventListener("complete", (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.url) {
          // ✅ Single seed done
          setProgressData((prev) => ({
            ...prev,
            [data.url]: {
              ...(prev[data.url] || {}),
              ...data,
              percent: 100,
              status: "completed",
            },
          }));
        } else {
          // ✅ All completed
          console.log("[SSE] All tasks complete.");
          setConnected(false);
          source.close();
        }
      } catch (err) {
        console.error("SSE parse error (complete):", err);
      }
    });

    source.onerror = (e) => {
      console.warn("[SSE] Connection lost:", e);
      setConnected(false);
      source.close();

      // Exponential backoff for retry
      const nextDelay = Math.min(5000 * Math.pow(2, retries), 30000);
      console.log(`[SSE] Retrying in ${(nextDelay / 1000).toFixed(1)}s`);
      reconnectTimeout.current = setTimeout(() => {
        setRetries((prev) => prev + 1);
        connectSSE();
      }, nextDelay);
    };
  };

  // --------- Mount & cleanup ----------
  useEffect(() => {
    connectSSE();

    return () => {
      if (sourceRef.current) {
        sourceRef.current.close();
      }
      if (reconnectTimeout.current) {
        clearTimeout(reconnectTimeout.current);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [requestId]);

  // --------- UI ----------
  const entries = Object.entries(progressData);

  const getBadgeClass = (status) => {
    switch (status) {
      case "completed":
        return "bg-success";
      case "processing":
        return "bg-info text-dark";
      case "started":
        return "bg-warning text-dark";
      default:
        return "bg-secondary";
    }
  };

  return (
    <div className="d-flex">
      <Sidebar />
      <div className="flex-grow-1 bg-light min-vh-100">
        <Topbar />
        <div className="container py-4">
          <div className="d-flex justify-content-between align-items-center mb-3">
            <h3 className="fw-bold text-primary mb-0">
              Live Crawl {requestId ? "Events" : "Dashboard"}
            </h3>
            <span
              className={`badge ${
                connected ? "bg-success" : "bg-danger"
              } px-3 py-2`}
            >
              {connected ? "Connected" : "Disconnected"}
            </span>
          </div>

          {entries.length === 0 ? (
            <div className="text-center text-muted py-5">
              <i className="bi bi-broadcast fs-2 d-block mb-2"></i>
              <p>No crawl updates yet. Waiting for events...</p>
              {!connected && (
                <small className="text-secondary">
                  Retrying connection... (attempt {retries})
                </small>
              )}
            </div>
          ) : (
            <div className="card shadow-sm">
              <div className="card-body">
                {entries.map(([url, data]) => (
                  <div key={url} className="mb-3 border-bottom pb-2">
                    <div className="d-flex justify-content-between align-items-center">
                      <div>
                        <strong className="text-break">{url}</strong>
                        <span
                          className={`badge ms-2 ${getBadgeClass(data.status)}`}
                        >
                          {data.status}
                        </span>
                      </div>
                      <small className="text-muted">
                        {data.done || 0}/{data.total || 0} pages
                      </small>
                    </div>

                    <div className="progress mt-2" style={{ height: "8px" }}>
                      <div
                        className={`progress-bar ${
                          data.status === "completed"
                            ? "bg-success"
                            : "progress-bar-striped progress-bar-animated"
                        }`}
                        style={{ width: `${data.percent?.toFixed(1) || 0}%` }}
                      ></div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default Events;
