// src/components/Sidebar.jsx
import { useNavigate } from "react-router-dom";

function Sidebar() {
  const navigate = useNavigate();

  return (
    <nav
      className="d-flex flex-column bg-gradient-primary text-white vh-100 p-3"
      style={{ width: "250px" }}
    >
      <h4 className="text-center mb-4 fw-bold">CCompliance</h4>

      <ul className="nav flex-column">
        <li className="nav-item mb-2">
          <button
            onClick={() => navigate("/dashboard")}
            className="btn btn-link text-white text-start w-100"
          >
            <i className="bi bi-speedometer2 me-2"></i> Dashboard
          </button>
        </li>
        <li className="nav-item mb-2">
          <button
            onClick={() => navigate("/tasks")}
            className="btn btn-link text-white text-start w-100"
          >
            <i className="bi bi-list-task me-2"></i> Tasks
          </button>
        </li>
        <li className="nav-item mb-2">
          <button
            onClick={() => navigate("/report")}
            className="btn btn-link text-white text-start w-100"
          >
            <i className="bi bi-bar-chart-line me-2"></i> Reports
          </button>
        </li>
      </ul>
    </nav>
  );
}

export default Sidebar;
