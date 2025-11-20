import { useState, useCallback, useRef } from "react";
import * as XLSX from "xlsx";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

const MAX_URLS_PER_FILE = 50; // Limit to 50 URLs per file for testing
const MAX_FILE_SIZE_MB = 10; // File size limit for testing
const BATCH_SIZE = 50; // match crawler's internal batchSize
const MAX_CONCURRENT_UPLOADS = 5;
const CHUNK_READ_SIZE = 1024 * 1024; // 1MB chunks for large files

/**
 * Dashboard - URL Upload Page for Testing
 * Features:
 * - Upload up to 50 URLs per file for testing
 * - File validation (size + URL count)
 * - Progress tracking
 * - Concurrent upload management
 */
function Dashboard() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState({ current: 0, total: 0 });
  const [fileStats, setFileStats] = useState({
    count: 0,
    valid: false,
    warning: "",
  });
  const [error, setError] = useState("");
  const [successMessage, setSuccessMessage] = useState("");
  const [uploadMode, setUploadMode] = useState("file"); // "file" or "manual"
  const [manualUrls, setManualUrls] = useState("");
  const fileInputRef = useRef(null);

  // ===============================
  // File Parsing Utilities
  // ===============================
  const parseCSV = useCallback((text) => {
    return text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.startsWith("http"));
  }, []);

  const parseXLSX = useCallback((data) => {
    const workbook = XLSX.read(data, { type: "array" });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
    return rows
      .flat()
      .filter((u) => typeof u === "string" && u.startsWith("http"));
  }, []);

  // ===============================
  // File Handling + Validation
  // ===============================
  const handleFileChange = useCallback(
    (e) => {
      const file = e.target.files?.[0];
      if (!file) return;

      // ✅ NEW: Check file type
      const validType = /\.(xlsx|csv)$/i.test(file.name);
      if (!validType) {
        setError("Invalid file type. Please upload .xlsx or .csv");
        setSelectedFile(null);
        setFileStats({ count: 0, valid: false, warning: "" });
        return;
      }

      // ✅ NEW: Check file size (100MB limit)
      const fileSizeMB = file.size / (1024 * 1024);
      if (fileSizeMB > MAX_FILE_SIZE_MB) {
        setError(
          `File too large (${fileSizeMB.toFixed(
            1
          )}MB > ${MAX_FILE_SIZE_MB}MB). Please reduce file size.`
        );
        setSelectedFile(null);
        setFileStats({ count: 0, valid: false, warning: "" });
        return;
      }

      setSelectedFile(file);
      setError("");
      setSuccessMessage("");

      const reader = new FileReader();
      reader.onload = (event) => {
        try {
          let urls = [];
          if (file.name.endsWith(".csv")) {
            const text = event.target.result;
            urls = parseCSV(text);
          } else {
            const data = new Uint8Array(event.target.result);
            urls = parseXLSX(data);
          }

          const count = urls.length;
          let warning = "";

          // Show warning if exceeding test limit
          if (count > MAX_URLS_PER_FILE) {
            warning = `⚠️ Exceeds test limit: ${count} URLs found. Maximum ${MAX_URLS_PER_FILE} URLs allowed for testing.`;
          } else if (count > 0) {
            warning = `ℹ️ Ready to upload ${count} URL(s) for testing.`;
          }

          setFileStats({
            count,
            valid: count > 0 && count <= MAX_URLS_PER_FILE,
            warning,
          });
        } catch (err) {
          console.error("[file:parse] Error:", err);
          setError("Failed to parse file. Please check format.");
          setFileStats({ count: 0, valid: false, warning: "" });
        }
      };

      reader.onerror = () => {
        setError("Failed to read file");
        setFileStats({ count: 0, valid: false, warning: "" });
      };

      if (file.name.endsWith(".csv")) reader.readAsText(file);
      else reader.readAsArrayBuffer(file);
    },
    [parseCSV, parseXLSX]
  );

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

    if (!response.ok)
      throw new Error(`Batch ${batchIndex} failed: ${response.status}`);

    const data = await response.json(); // { status, request_id }
    console.log(`Batch ${batchIndex}/${totalBatches} uploaded:`, data);

    setProgress((prev) => ({
      current: prev.current + 1,
      total: totalBatches,
    }));
  };

  const handleUpload = async () => {
    let urls = [];

    if (uploadMode === "manual") {
      // Parse manual URLs from textarea
      urls = manualUrls
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.startsWith("http"));

      if (urls.length === 0) {
        alert("Please enter at least one valid URL (starting with http:// or https://)");
        return;
      }

      if (urls.length > MAX_URLS_PER_FILE) {
        alert(`Too many URLs. Maximum ${MAX_URLS_PER_FILE} URLs allowed.`);
        return;
      }
    } else {
      // File upload mode
      if (!selectedFile) return alert("Please select a file first.");
      if (!fileStats.valid)
        return alert(`Invalid file or exceeds ${MAX_URLS_PER_FILE} URLs.`);

      const ext = selectedFile.name.split(".").pop().toLowerCase();
      const reader = new FileReader();

      return new Promise((resolve) => {
        reader.onload = async (e) => {
          try {
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
              urls = rows
                .flat()
                .filter((u) => typeof u === "string" && u.startsWith("http"));
            }

            if (urls.length === 0) {
              alert("No valid URLs found in file.");
              setUploading(false);
              resolve();
              return;
            }

            await processUrls(urls);
            resolve();
          } catch (err) {
            console.error("Upload failed:", err);
            alert("Upload failed. Please check logs.");
            setUploading(false);
            resolve();
          }
        };

        if (ext === "csv") reader.readAsText(selectedFile);
        else reader.readAsArrayBuffer(selectedFile);
      });
    }

    await processUrls(urls);
  };

  const processUrls = async (urls) => {
    setUploading(true);
    setProgress({ current: 0, total: 0 });
    setError("");
    setSuccessMessage("");

    try {
      // split into batches (match crawler's batch handling)
      const batches = [];
      for (let i = 0; i < urls.length; i += BATCH_SIZE)
        batches.push(urls.slice(i, i + BATCH_SIZE));

      setProgress({ current: 0, total: batches.length });
      console.log(
        `Uploading ${urls.length} URLs in ${batches.length} batches`
      );

      // Manage concurrency
      const queue = [...batches];
      const active = [];

      const runNext = async () => {
        if (!queue.length) return;
        const batch = queue.shift();
        const index = progress.current + 1;
        const task = uploadBatch(batch, index, batches.length)
          .catch((err) => {
            console.error("Batch upload error:", err);
            setError(`Batch ${index} failed: ${err.message}`);
          })
          .finally(() => {
            active.splice(active.indexOf(task), 1);
            runNext();
          });
        active.push(task);
      };

      const concurrency = Math.min(MAX_CONCURRENT_UPLOADS, batches.length);
      await Promise.all(Array.from({ length: concurrency }, runNext));
      await Promise.all(active);

      setSuccessMessage(
        `Upload completed! ${urls.length} URLs in ${batches.length} batches sent to crawler.`
      );
    } catch (err) {
      console.error("Upload failed:", err);
      setError(`Upload failed: ${err.message}`);
    } finally {
      setUploading(false);
      if (uploadMode === "file") {
        setSelectedFile(null);
        setFileStats({ count: 0, valid: false });
      } else {
        setManualUrls("");
      }
      setProgress({ current: 0, total: 0 });
    }
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
            <i className="bi bi-cloud-upload me-2"></i>URL Upload & Crawl
          </h3>

          <div className="row g-4">
            {/* Upload Section */}
            <div className="col-12 col-lg-8 mx-auto">
              <div className="card border-0 shadow-sm p-4">
                <h5 className="fw-semibold text-primary mb-3">
                  <i className="bi bi-cloud-upload me-2"></i>Upload URLs
                </h5>
                
                {/* Mode Toggle */}
                <div className="btn-group w-100 mb-3" role="group">
                  <input
                    type="radio"
                    className="btn-check"
                    name="uploadMode"
                    id="mode-file"
                    checked={uploadMode === "file"}
                    onChange={() => setUploadMode("file")}
                    disabled={uploading}
                  />
                  <label className="btn btn-outline-primary" htmlFor="mode-file">
                    <i className="bi bi-file-earmark-excel me-2"></i>File Upload
                  </label>

                  <input
                    type="radio"
                    className="btn-check"
                    name="uploadMode"
                    id="mode-manual"
                    checked={uploadMode === "manual"}
                    onChange={() => setUploadMode("manual")}
                    disabled={uploading}
                  />
                  <label className="btn btn-outline-primary" htmlFor="mode-manual">
                    <i className="bi bi-pencil-square me-2"></i>Manual Entry
                  </label>
                </div>

                {uploadMode === "file" ? (
                  <>
                    <div className="alert alert-info mb-3">
                      <i className="bi bi-info-circle me-2"></i>
                      <strong>For Testing:</strong> Please upload a file with up to {MAX_URLS_PER_FILE} URLs.
                      <br />
                      <small>Supports <strong>.xlsx</strong> and <strong>.csv</strong> files with one URL per row.</small>
                    </div>

                    <input
                      ref={fileInputRef}
                      type="file"
                      className="form-control mb-3"
                      accept=".xlsx,.csv"
                      onChange={handleFileChange}
                      disabled={uploading}
                    />
                  </>
                ) : (
                  <>
                    <div className="alert alert-info mb-3">
                      <i className="bi bi-info-circle me-2"></i>
                      <strong>For Testing:</strong> Enter up to {MAX_URLS_PER_FILE} URLs manually (one per line).
                    </div>

                    <textarea
                      className="form-control mb-3"
                      rows="8"
                      placeholder="https://example.com/&#10;https://another-site.com/page"
                      value={manualUrls}
                      onChange={(e) => setManualUrls(e.target.value)}
                      disabled={uploading}
                      style={{ fontFamily: "monospace", fontSize: "0.9rem" }}
                    />

                    {manualUrls && (
                      <div className="alert alert-info small mb-3">
                        <i className="bi bi-info-circle me-2"></i>
                        {manualUrls
                          .split(/\r?\n/)
                          .filter((line) => line.trim().startsWith("http")).length}{" "}
                        valid URL(s) detected (max {MAX_URLS_PER_FILE} allowed)
                      </div>
                    )}
                  </>
                )}

                <button
                  className="btn btn-primary w-100"
                  onClick={handleUpload}
                  disabled={
                    uploading ||
                    (uploadMode === "file" && !fileStats.valid) ||
                    (uploadMode === "manual" && !manualUrls.trim())
                  }
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

                {uploadMode === "file" && selectedFile && !uploading && (
                  <div
                    className={`alert mt-3 small text-center ${
                      fileStats.valid ? "alert-info" : "alert-warning"
                    }`}
                  >
                    <strong>{selectedFile.name}</strong> — {fileStats.count}{" "}
                    URLs detected
                    {!fileStats.valid &&
                      fileStats.count > MAX_URLS_PER_FILE && (
                        <div className="mt-2 small">
                          ⚠️ Exceeds {MAX_URLS_PER_FILE} URL limit
                        </div>
                      )}
                  </div>
                )}

                {/* Show file stats warning/info */}
                {fileStats.warning && !uploading && (
                  <div className={`alert mt-3 small mb-0 ${
                    fileStats.count > MAX_URLS_PER_FILE ? 'alert-warning' : 'alert-info'
                  }`}>
                    <i className={`bi me-2 ${fileStats.count > MAX_URLS_PER_FILE ? 'bi-exclamation-triangle' : 'bi-info-circle'}`}></i>
                    {fileStats.warning}
                  </div>
                )}

                {uploading && progress.total > 0 && (
                  <div className="mt-3">
                    <div className="d-flex justify-content-between mb-1 small">
                      <span>Upload Progress</span>
                      <span className="fw-semibold">
                        {progress.current}/{progress.total}
                      </span>
                    </div>
                    <div className="progress" style={{ height: "8px" }}>
                      <div
                        className="progress-bar progress-bar-striped progress-bar-animated bg-success"
                        style={{
                          width: `${
                            (progress.current / progress.total) * 100
                          }%`,
                        }}
                      ></div>
                    </div>
                  </div>
                )}

                {error && (
                  <div className="alert alert-danger mt-3 small mb-0">
                    <i className="bi bi-exclamation-circle me-2"></i>
                    {error}
                  </div>
                )}

                {successMessage && (
                  <div className="alert alert-success mt-3 small mb-0">
                    <i className="bi bi-check-circle me-2"></i>
                    {successMessage}
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
