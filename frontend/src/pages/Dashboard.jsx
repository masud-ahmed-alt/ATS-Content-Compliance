import { useState, useEffect } from "react";
import * as XLSX from "xlsx";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

function Dashboard() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [uploading, setUploading] = useState(false);

  // ✅ Fetch recent tasks
  const fetchRecentTasks = async () => {
    try {
      const response = await fetch(`${API_CONFIG.fetcherBaseUrl}/tasks`);
      if (!response.ok) throw new Error(`HTTP error! ${response.status}`);
      const data = await response.json();
      setTasks(data);
    } catch (err) {
      console.error("Error fetching recent tasks:", err);
    }
  };

  useEffect(() => {
    fetchRecentTasks();
    const interval = setInterval(fetchRecentTasks, 10000);
    return () => clearInterval(interval);
  }, []);

  // ✅ Validate and store file
  const handleFileChange = (e) => {
    const file = e.target.files[0];
    if (file && !file.name.match(/\.(xlsx|csv)$/i)) {
      alert("Invalid file type! Please select an XLSX or CSV file.");
      e.target.value = "";
      setSelectedFile(null);
      return;
    }
    setSelectedFile(file);
  };

  // ✅ Read URLs from CSV/XLSX and upload to Go Fetcher
  const handleUpload = async () => {
    if (!selectedFile) return alert("Please select a file first.");
    setUploading(true);

    try {
      const fileExtension = selectedFile.name.split(".").pop().toLowerCase();
      const reader = new FileReader();

      reader.onload = async (e) => {
        let urls = [];

        try {
          if (fileExtension === "csv") {
            // Parse CSV manually
            const text = e.target.result;
            urls = text
              .split(/\r?\n/)
              .map((line) => line.trim())
              .filter((line) => line && line.startsWith("http"));
          } else {
            // Parse XLSX
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: "array" });
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
            urls = rows.flat().filter((url) => typeof url === "string" && url.startsWith("http"));
          }

          if (urls.length === 0) {
            alert("No valid URLs found in the file.");
            setUploading(false);
            return;
          }

          console.log("Uploading URLs:", urls);

          const payload = { urls };
          const response = await fetch(`${API_CONFIG.fetcherBaseUrl}/fetch`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });

          if (!response.ok) {
            const text = await response.text();
            throw new Error(`Upload failed: ${text}`);
          }

          const result = await response.json();
          console.log("Upload result:", result);
          alert(`Uploaded ${urls.length} URLs successfully!`);
          fetchRecentTasks();
        } catch (error) {
          console.error("Error parsing/uploading file:", error);
          alert("Error uploading file. Check console for details.");
        } finally {
          setUploading(false);
          setSelectedFile(null);
        }
      };

      if (fileExtension === "csv") {
        reader.readAsText(selectedFile);
      } else {
        reader.readAsArrayBuffer(selectedFile);
      }
    } catch (err) {
      console.error(err);
      alert("Unexpected error while uploading.");
      setUploading(false);
    }
  };

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
                  ⚠️ Only <strong>.xlsx</strong> or <strong>.csv</strong> files are allowed.
                </p>

                <input
                  type="file"
                  className="form-control mb-3"
                  accept=".xlsx,.csv"
                  onChange={handleFileChange}
                />

                <button
                  onClick={handleUpload}
                  className="btn btn-primary w-100"
                  disabled={!selectedFile || uploading}
                >
                  {uploading ? (
                    <>
                      <span
                        className="spinner-border spinner-border-sm me-2"
                        role="status"
                        aria-hidden="true"
                      ></span>
                      Uploading...
                    </>
                  ) : (
                    <>
                      <i className="bi bi-upload me-2"></i> Upload File
                    </>
                  )}
                </button>

                {selectedFile && !uploading && (
                  <div className="alert alert-info mt-3 mb-0 p-2 small text-center text-lg-start">
                    Selected File: <strong>{selectedFile.name}</strong>
                  </div>
                )}
              </div>
            </div>

            {/* RIGHT: Recent Tasks Section */}
            <div className="col-12 col-lg-7">
              <div className="card shadow-sm border-0 h-100">
                <div className="card-body p-3 p-md-4">
                  <div className="d-flex flex-column flex-md-row justify-content-between align-items-center mb-3">
                    <h5 className="fw-bold text-primary mb-2 mb-md-0">
                      Recent Tasks
                    </h5>
                    <button
                      onClick={fetchRecentTasks}
                      className="btn btn-sm btn-outline-secondary"
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
