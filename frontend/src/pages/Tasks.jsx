import Sidebar from "../components/Sidebar";
import Topbar from "../components/Topbar";

function Tasks() {
  return (
    <div className="d-flex">
      <Sidebar />
      <div className="flex-grow-1 bg-light min-vh-100">
        <Topbar />
        <div className="container py-4">
          <h3 className="fw-bold text-primary mb-3">Tasks</h3>
          <div className="card shadow-sm">
            <div className="card-body">
              <p>Here you can manage your assigned tasks.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
export default Tasks;
