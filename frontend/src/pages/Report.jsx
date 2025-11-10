import { useEffect, useState } from "react";
import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";
import { API_CONFIG } from "../utils/apiConfig";

function Report() {
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);

  // ✅ Fetch all grouped report tasks
  const fetchReportTasks = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${API_CONFIG.analyzerBaseUrl}/report/tasks`);
      if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);
      const data = await response.json();
      setTasks(data.tasks || []);
    } catch (error) {
      console.error("Error fetching report tasks:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchReportTasks();
  }, []);

  return (
    <div className="d-flex flex-column flex-lg-row">
      <Sidebar />

      <div className="flex-grow-1 bg-light min-vh-100">
        <Topbar />

        <div className="container-fluid py-4 px-3 px-md-4">
          <div className="d-flex justify-content-between align-items-center mb-3">
            <h3 className="fw-bold text-primary mb-0">Reports</h3>
            <button
              className="btn btn-sm btn-outline-secondary"
              onClick={fetchReportTasks}
              disabled={loading}
            >
              <i className="bi bi-arrow-clockwise me-1"></i>
              {loading ? "Refreshing..." : "Refresh"}
            </button>
          </div>

          <div className="card shadow-sm border-0">
            <div className="card-body p-3 p-md-4">
              {loading ? (
                <div className="text-center py-5">
                  <div className="spinner-border text-primary mb-3" role="status"></div>
                  <p className="text-muted">Loading report tasks...</p>
                </div>
              ) : tasks.length > 0 ? (
                <div className="table-responsive">
                  <table className="table table-striped align-middle">
                    <thead className="table-light">
                      <tr>
                        <th style={{ width: "60px" }}>#</th>
                        <th>Main URL</th>
                        <th style={{ width: "120px" }}>Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tasks.map((task, index) => (
                        <tr key={task.main_url}>
                          <td>{index + 1}</td>
                          <td className="text-break">
                            <a
                              href={task.main_url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-decoration-none"
                            >
                              {task.main_url}
                            </a>
                          </td>
                          <td>
                            <a
                              href={`/report/${encodeURIComponent(task.main_url)}`}
                              className="btn btn-sm btn-outline-primary"
                            >
                              <i className="bi bi-eye me-1"></i> View Details
                            </a>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-center text-muted py-5">
                  <i className="bi bi-inbox fs-3 d-block mb-2"></i>
                  <p className="mb-0">No report tasks found.</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Report;
