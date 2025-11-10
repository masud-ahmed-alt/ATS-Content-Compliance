import { Routes, Route, Navigate } from "react-router-dom";
import LoginPage from "./pages/Login";
import Dashboard from "./pages/Dashboard";
import Report from "./pages/Report";
import ReportDetails from "./pages/ReportDetails";
import NotFound from "./pages/NotFound";
import ProtectedRoute from "./components/ProtectedRoute";
import Events from "./pages/Events";

function App() {
  return (
    <Routes>
      {/* Public route */}
      <Route path="/login" element={<LoginPage />} />

      {/* Home route */}
      <Route
        path="/"
        element={
          <ProtectedRoute>
            <Dashboard />
          </ProtectedRoute>
        }
      />

      {/* Protected routes */}
      <Route
        path="/dashboard"
        element={
          <ProtectedRoute>
            <Dashboard />
          </ProtectedRoute>
        }
      />

      <Route
        path="/events"
        element={
          <ProtectedRoute>
            <Events />
          </ProtectedRoute>
        }
      />

      <Route
        path="/report"
        element={
          <ProtectedRoute>
            <Report />
          </ProtectedRoute>
        }
      />

      {/* ✅ details by main URL (encoded) */}
      <Route
        path="/report/:mainUrl"
        element={
          <ProtectedRoute>
            <ReportDetails />
          </ProtectedRoute>
        }
      />

      {/* Fallback route */}
      <Route path="*" element={<NotFound />} />
    </Routes>
  );
}

export default App;
