// src/components/Sidebar.jsx
import { useNavigate, useLocation } from "react-router-dom";

function Sidebar() {
  const navigate = useNavigate();
  const location = useLocation();

  const isActive = (path) => location.pathname === path;

  return (
    <nav
      className="d-flex flex-column bg-gradient-primary text-white vh-100 p-3 position-fixed"
      style={{ width: "250px", overflowY: "auto" }}
    >
      <h4 className="text-center mb-4 fw-bold">
        <i className="bi bi-shield-check me-2"></i>CCompliance
      </h4>

      <ul className="nav flex-column">
        <li className="nav-item mb-2">
          <button
            onClick={() => navigate("/dashboard")}
            className={`btn btn-link text-white text-start w-100 ${
              isActive("/") || isActive("/dashboard") ? "fw-bold" : ""
            }`}
          >
            <i className="bi bi-speedometer2 me-2"></i> Dashboard
          </button>
        </li>
        <li className="nav-item mb-2">
          <button
            onClick={() => navigate("/events")}
            className={`btn btn-link text-white text-start w-100 ${
              isActive("/events") ? "fw-bold" : ""
            }`}
          >
            <i className="bi bi-broadcast me-2"></i> Live Events
          </button>
        </li>
        <li className="nav-item mb-2">
          <button
            onClick={() => navigate("/report")}
            className={`btn btn-link text-white text-start w-100 ${
              isActive("/report") ? "fw-bold" : ""
            }`}
          >
            <i className="bi bi-bar-chart-line me-2"></i> Reports
          </button>
        </li>
      </ul>

      <div className="mt-auto pt-4 border-top border-secondary">
        <small className="text-white-50 d-block text-center">
          Content Compliance Scanner v1.0
        </small>
      </div>
    </nav>
  );
}

export default Sidebar;
