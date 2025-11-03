import React from "react";
import { useNavigate } from "react-router-dom";

const NotFound = () => {
  const navigate = useNavigate();

  const handleGoBack = () => {
    const isAuthenticated = localStorage.getItem("auth") === "true";
    navigate(isAuthenticated ? "/dashboard" : "/login");
  };

  return (
    <div
      className="d-flex flex-column justify-content-center align-items-center vh-100 text-center bg-light"
    >
      <div className="mb-4">
        <h1 className="display-1 fw-bold text-primary">404</h1>
        <h3 className="fw-semibold text-dark mb-3">Page Not Found</h3>
        <p className="text-muted mb-4" style={{ maxWidth: "400px" }}>
          Oops! The page you’re looking for doesn’t exist or has been moved.
          Please check the URL or go back to your dashboard.
        </p>
        <button
          onClick={handleGoBack}
          className="btn btn-primary px-4 py-2 shadow-sm"
        >
          <i className="bi bi-arrow-left-circle me-2"></i>
          Back to Dashboard
        </button>
      </div>

      {/* Decorative SVG illustration */}
      <div style={{ maxWidth: "500px", opacity: 0.85 }}>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 700 400"
          fill="none"
          className="img-fluid"
        >
          <path
            d="M100 300 C150 150, 550 150, 600 300"
            stroke="#4e73df"
            strokeWidth="5"
            fill="transparent"
          />
          <circle cx="200" cy="250" r="25" fill="#1cc88a" />
          <circle cx="500" cy="250" r="25" fill="#f6c23e" />
          <text
            x="230"
            y="255"
            fontSize="80"
            fill="#858796"
            fontWeight="bold"
          >
            404
          </text>
        </svg>
      </div>
    </div>
  );
};

export default NotFound;
