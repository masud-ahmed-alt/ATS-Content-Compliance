import { useState, useEffect, useRef, useCallback } from "react";
import * as XLSX from "xlsx";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

const MAX_URLS_PER_FILE = 10000;
const BATCH_SIZE = 50;
const MAX_CONCURRENT_UPLOADS = 5; // adjust based on backend capacity

function Dashboard() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState({ current: 0, total: 0 });
  const [fileStats, setFileStats] = useState({ count: 0, valid: false });
  const abortRef = useRef(null);

  // ===================================
  // Fetch recent tasks with interval
  // ===================================
  const fetchRecentTasks = useCallback(async (signal) => {
    try {
      const response = await fetch(`${API_CONFIG.fetcherBaseUrl}/tasks`, { signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      setTasks(data);
    } catch (err) {
      if (err.name !== "AbortError") console.error("Fetch tasks failed:", err);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    abortRef.current = controller;
    fetchRecentTasks(controller.signal);

    const interval = setInterval(() => fetchRecentTasks(controller.signal), 10000);

    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [fetchRecentTasks]);

  // ===================================
  // File handling and validation
  // ===================================
  const handleFileChange = (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const isValidType = /\.(xlsx|csv)$/i.test(file.name);
    if (!isValidType) {
      alert("Invalid file type. Please select an XLSX or CSV file.");
      e.target.value = "";
      setSelectedFile(null);
      setFileStats({ count: 0, valid: false });
      return;
    }

    setSelectedFile(file);
    const reader = new FileReader();

    reader.onload = (event) => {
      try {
        let urls = [];

        if (file.name.endsWith(".csv")) {
          const text = event.target.result;
          urls = text
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter((line) => line.startsWith("http"));
        } else {
          const data = new Uint8Array(event.target.result);
          const workbook = XLSX.read(data, { type: "array" });
          const sheet = workbook.Sheets[workbook.SheetNames[0]];
          const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
          urls = rows.flat().filter((url) => typeof url === "string" && url.startsWith("http"));
        }

        const count = urls.length;
        const valid = count > 0 && count <= MAX_URLS_PER_FILE;
        setFileStats({ count, valid });
      } catch (err) {
        console.error("File preview error:", err);
        setFileStats({ count: 0, valid: false });
      }
    };

    if (file.name.endsWith(".csv")) reader.readAsText(file);
    else reader.readAsArrayBuffer(file);
  };

  // ===================================
  // Batch Upload Logic (Parallel + Scalable)
  // ===================================
  const uploadBatch = async (batchUrls, batchIndex, totalBatches) => {
    const payload = { urls: batchUrls };
    const response = await fetch(`${API_CONFIG.fetcherBaseUrl}/fetch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Batch ${batchIndex} failed: ${text}`);
    }

    const result = await response.json();
    setProgress((prev) => ({
      current: prev.current + 1,
      total: totalBatches,
    }));
    console.log(`Batch ${batchIndex}/${totalBatches} completed.`);
    return result;
  };

  const handleUpload = async () => {
    if (!selectedFile) {
      alert("Please select a file first.");
      return;
    }
    if (!fileStats.valid) {
      alert(`Invalid file or exceeds ${MAX_URLS_PER_FILE} URLs.`);
      return;
    }

    setUploading(true);
    setProgress({ current: 0, total: 0 });

    const extension = selectedFile.name.split(".").pop().toLowerCase();
    const reader = new FileReader();

    reader.onload = async (e) => {
      try {
        let urls = [];

        // Parse CSV or XLSX
        if (extension === "csv") {
          const text = e.target.result;
          urls = text
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter((line) => line.startsWith("http"));
        } else {
          const data = new Uint8Array(e.target.result);
          const workbook = XLSX.read(data, { type: "array" });
          const sheet = workbook.Sheets[workbook.SheetNames[0]];
          const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
          urls = rows.flat().filter((url) => typeof url === "string" && url.startsWith("http"));
        }

        if (!urls.length) {
          alert("No valid URLs found in file.");
          setUploading(false);
          return;
        }

        if (urls.length > MAX_URLS_PER_FILE) {
          alert(`Maximum ${MAX_URLS_PER_FILE} URLs per upload.`);
          setUploading(false);
          return;
        }

        // Split into batches
        const batches = [];
        for (let i = 0; i < urls.length; i += BATCH_SIZE) {
          batches.push(urls.slice(i, i + BATCH_SIZE));
        }

        setProgress({ current: 0, total: batches.length });
        console.log(`Uploading ${urls.length} URLs in ${batches.length} batches...`);

        // Upload in parallel (limited concurrency)
        const queue = [...batches];
        const active = [];

        const runNext = async () => {
          if (!queue.length) return;
          const batch = queue.shift();
          const index = progress.current + 1;
          const task = uploadBatch(batch, index, batches.length)
            .catch((err) => console.error("Batch upload error:", err))
            .finally(() => active.splice(active.indexOf(task), 1))
            .then(runNext);
          active.push(task);
        };

        // Launch concurrent uploads
        const concurrency = Math.min(MAX_CONCURRENT_UPLOADS, batches.length);
        await Promise.all(Array.from({ length: concurrency }, runNext));
        await Promise.all(active);

        alert(`Upload completed: ${urls.length} URLs in ${batches.length} batches.`);
        fetchRecentTasks();
      } catch (err) {
        console.error("Upload failed:", err);
        alert("Upload failed. Please check logs.");
      } finally {
        setUploading(false);
        setSelectedFile(null);
        setFileStats({ count: 0, valid: false });
        setProgress({ current: 0, total: 0 });
      }
    };

    if (extension === "csv") reader.readAsText(selectedFile);
    else reader.readAsArrayBuffer(selectedFile);
  };

  // ===================================
  // UI Rendering
  // ===================================
  return (
    <div className="d-flex flex-column flex-lg-row">
      <Sidebar />

      <div className="flex-grow-1 bg-light min-vh-100">
        <Topbar />

        <div className="container-fluid py-4 px-3 px-md-4">
          <h3 className="fw-bold text-primary mb-4 text-center text-lg-start">
            Dashboard
          </h3>

          <div className="row g-4">
            {/* LEFT: Upload Section */}
            <div className="col-12 col-lg-5">
              <div className="card shadow-sm border-0 p-4 h-100">
                <h5 className="mb-3 fw-semibold text-primary text-center text-lg-start">
                  Upload Data File
                </h5>
                <p className="text-danger fw-semibold small text-center text-lg-start">
                  Supports .xlsx and .csv files.<br />Maximum {MAX_URLS_PER_FILE} URLs per upload.
                </p>

                <input
                  type="file"
                  className="form-control mb-3"
                  accept=".xlsx,.csv"
                  onChange={handleFileChange}
                  disabled={uploading}
                />

                <button
                  onClick={handleUpload}
                  className="btn btn-primary w-100"
                  disabled={!selectedFile || uploading || !fileStats.valid}
                >
                  {uploading ? (
                    <>
                      <span
                        className="spinner-border spinner-border-sm me-2"
                        role="status"
                        aria-hidden="true"
                      ></span>
                      Uploading ({progress.current}/{progress.total})
                    </>
                  ) : (
                    <>
                      <i className="bi bi-upload me-2"></i> Upload File
                    </>
                  )}
                </button>

                {selectedFile && !uploading && (
                  <div
                    className={`alert mt-3 mb-0 p-2 small text-center text-lg-start ${
                      fileStats.valid ? "alert-info" : "alert-warning"
                    }`}
                  >
                    <strong>{selectedFile.name}</strong> — {fileStats.count} URLs detected{" "}
                    {!fileStats.valid && "(Invalid or too many)"}
                  </div>
                )}

                {uploading && progress.total > 0 && (
                  <div className="progress mt-3" style={{ height: "6px" }}>
                    <div
                      className="progress-bar progress-bar-striped progress-bar-animated bg-success"
                      style={{ width: `${(progress.current / progress.total) * 100}%` }}
                    ></div>
                  </div>
                )}
              </div>
            </div>

            {/* RIGHT: Recent Tasks */}
            <div className="col-12 col-lg-7">
              <div className="card shadow-sm border-0 h-100">
                <div className="card-body p-3 p-md-4">
                  <div className="d-flex flex-column flex-md-row justify-content-between align-items-center mb-3">
                    <h5 className="fw-bold text-primary mb-2 mb-md-0">Recent Tasks</h5>
                    <button
                      onClick={() => fetchRecentTasks()}
                      className="btn btn-sm btn-outline-secondary"
                      disabled={uploading}
                    >
                      <i className="bi bi-arrow-clockwise me-1"></i> Refresh
                    </button>
                  </div>

                  {tasks.length > 0 ? (
                    <div className="table-responsive">
                      <table className="table table-striped align-middle">
                        <thead className="table-light">
                          <tr>
                            <th>ID</th>
                            <th>Main URL</th>
                            <th>Status</th>
                            <th>Started At</th>
                          </tr>
                        </thead>
                        <tbody>
                          {tasks.map((task) => (
                            <tr key={task.id}>
                              <td className="text-truncate" style={{ maxWidth: "150px" }}>
                                {task.id}
                              </td>
                              <td className="text-break">
                                <a
                                  href={task.main_url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                >
                                  {task.main_url}
                                </a>
                              </td>
                              <td>
                                <span
                                  className={`badge ${
                                    task.status === "completed"
                                      ? "bg-success"
                                      : task.status === "processing"
                                      ? "bg-warning text-dark"
                                      : "bg-secondary"
                                  }`}
                                >
                                  {task.status}
                                </span>
                              </td>
                              <td>{new Date(task.started_at).toLocaleString()}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="text-center text-muted py-3">
                      <i className="bi bi-inbox fs-3 d-block mb-2"></i>
                      <p className="mb-0">No recent tasks found.</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;
