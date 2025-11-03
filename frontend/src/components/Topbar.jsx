// src/components/Topbar.jsx
import { useNavigate } from "react-router-dom";

function Topbar() {
  const navigate = useNavigate();

  const handleLogout = () => {
    localStorage.removeItem("auth");
    navigate("/login");
  };

  return (
    <nav className="navbar navbar-expand navbar-light bg-white topbar mb-4 shadow-sm">
      <div className="container-fluid d-flex justify-content-between align-items-center">
        <h5 className="fw-bold text-primary mb-0">Dashboard</h5>

        <button className="btn btn-outline-danger btn-sm" onClick={handleLogout}>
          <i className="bi bi-box-arrow-right me-1"></i> Logout
        </button>
      </div>
    </nav>
  );
}

export default Topbar;
