// src/components/Topbar.jsx
import { useNavigate, useLocation } from "react-router-dom";

function Topbar() {
  const navigate = useNavigate();
  const location = useLocation();

  const handleLogout = () => {
    localStorage.removeItem("auth");
    navigate("/login");
  };

  // Dynamic title based on current route
  const getPageTitle = () => {
    const path = location.pathname;
    if (path === "/" || path === "/dashboard") return "Dashboard";
    if (path === "/events") return "Live Events";
    if (path === "/report") return "Reports";
    if (path.startsWith("/report/")) return "Report Details";
    return "Dashboard";
  };

  return (
    <nav className="navbar navbar-expand navbar-light bg-white topbar mb-4 shadow-sm">
      <div className="container-fluid d-flex justify-content-between align-items-center">
        <h5 className="fw-bold text-primary mb-0">
          <i className="bi bi-speedometer2 me-2"></i>
          {getPageTitle()}
        </h5>

        <button
          className="btn btn-outline-danger btn-sm"
          onClick={handleLogout}
        >
          <i className="bi bi-box-arrow-right me-1"></i> Logout
        </button>
      </div>
    </nav>
  );
}

export default Topbar;
