import { useState } from "react";
import * as XLSX from "xlsx";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

const MAX_URLS_PER_FILE = 10000;
const BATCH_SIZE = 50; // match crawler's internal batchSize
const MAX_CONCURRENT_UPLOADS = 5;

function Dashboard() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState({ current: 0, total: 0 });
  const [fileStats, setFileStats] = useState({ count: 0, valid: false });

  // ===============================
  // File Handling + Validation
  // ===============================
  const handleFileChange = (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const validType = /\.(xlsx|csv)$/i.test(file.name);
    if (!validType) {
      alert("Invalid file type. Please upload .xlsx or .csv");
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
          urls = rows.flat().filter((u) => typeof u === "string" && u.startsWith("http"));
        }
        const count = urls.length;
        setFileStats({ count, valid: count > 0 && count <= MAX_URLS_PER_FILE });
      } catch (err) {
        console.error("File read error:", err);
        setFileStats({ count: 0, valid: false });
      }
    };

    if (file.name.endsWith(".csv")) reader.readAsText(file);
    else reader.readAsArrayBuffer(file);
  };

  // ===============================
  // Upload Logic (Batch + Concurrency)
  // ===============================
  const uploadBatch = async (urls, batchIndex, totalBatches) => {
    const payload = { urls };
    const response = await fetch(`${API_CONFIG.fetcherBaseUrl}/fetch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) throw new Error(`Batch ${batchIndex} failed: ${response.status}`);

    await response.json(); // { status, request_id }

    setProgress((prev) => ({
      current: prev.current + 1,
      total: totalBatches,
    }));
    console.log(`Batch ${batchIndex}/${totalBatches} uploaded successfully`);
  };

  const handleUpload = async () => {
    if (!selectedFile) return alert("Please select a file first.");
    if (!fileStats.valid)
      return alert(`Invalid file or exceeds ${MAX_URLS_PER_FILE} URLs.`);

    setUploading(true);
    setProgress({ current: 0, total: 0 });

    const ext = selectedFile.name.split(".").pop().toLowerCase();
    const reader = new FileReader();

    reader.onload = async (e) => {
      try {
        let urls = [];
        if (ext === "csv") {
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
          urls = rows.flat().filter((u) => typeof u === "string" && u.startsWith("http"));
        }

        if (urls.length === 0) {
          alert("No valid URLs found in file.");
          setUploading(false);
          return;
        }

        // split into batches (match crawler’s batch handling)
        const batches = [];
        for (let i = 0; i < urls.length; i += BATCH_SIZE)
          batches.push(urls.slice(i, i + BATCH_SIZE));

        setProgress({ current: 0, total: batches.length });
        console.log(`Uploading ${urls.length} URLs in ${batches.length} batches`);

        // Manage concurrency
        const queue = [...batches];
        const active = [];

        const runNext = async () => {
          if (!queue.length) return;
          const batch = queue.shift();
          const index = progress.current + 1;
          const task = uploadBatch(batch, index, batches.length)
            .catch((err) => console.error("Batch upload error:", err))
            .finally(() => {
              active.splice(active.indexOf(task), 1);
              runNext();
            });
          active.push(task);
        };

        const concurrency = Math.min(MAX_CONCURRENT_UPLOADS, batches.length);
        await Promise.all(Array.from({ length: concurrency }, runNext));
        await Promise.all(active);

        alert(`Upload completed (${urls.length} URLs in ${batches.length} batches)`);
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

    if (ext === "csv") reader.readAsText(selectedFile);
    else reader.readAsArrayBuffer(selectedFile);
  };

  // ===============================
  // UI
  // ===============================
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
            {/* Upload Section */}
            <div className="col-12 col-lg-6 mx-auto">
              <div className="card border-0 shadow-sm p-4">
                <h5 className="fw-semibold text-primary mb-3">Upload URLs File</h5>
                <p className="text-muted small mb-3">
                  Supports <strong>.xlsx</strong> and <strong>.csv</strong> files (up to{" "}
                  {MAX_URLS_PER_FILE} URLs). Each batch sends {BATCH_SIZE} URLs to crawler.
                </p>

                <input
                  type="file"
                  className="form-control mb-3"
                  accept=".xlsx,.csv"
                  onChange={handleFileChange}
                  disabled={uploading}
                />

                <button
                  className="btn btn-primary w-100"
                  onClick={handleUpload}
                  disabled={!fileStats.valid || uploading}
                >
                  {uploading ? (
                    <>
                      <span className="spinner-border spinner-border-sm me-2" />
                      Uploading ({progress.current}/{progress.total})
                    </>
                  ) : (
                    <>
                      <i className="bi bi-upload me-2"></i> Start Upload
                    </>
                  )}
                </button>

                {selectedFile && !uploading && (
                  <div
                    className={`alert mt-3 small text-center ${
                      fileStats.valid ? "alert-info" : "alert-warning"
                    }`}
                  >
                    <strong>{selectedFile.name}</strong> — {fileStats.count} URLs detected
                  </div>
                )}

                {uploading && progress.total > 0 && (
                  <div className="progress mt-3" style={{ height: "6px" }}>
                    <div
                      className="progress-bar progress-bar-striped progress-bar-animated bg-success"
                      style={{
                        width: `${(progress.current / progress.total) * 100}%`,
                      }}
                    ></div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;
