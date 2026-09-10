import { NavLink, Outlet } from "react-router-dom";
import { API_BASE_URL } from "../api/client";

const NAV_ITEMS = [
  { to: "/", label: "Overview", end: true },
  { to: "/incidents", label: "Incidents" },
  { to: "/approvals", label: "Approvals" },
  { to: "/audit", label: "Audit Log" },
];

export function Layout() {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-dot" />
          PipelineMedic AI
        </div>
        <nav>
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer">
          <div>API</div>
          <code>{API_BASE_URL}</code>
        </div>
      </aside>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
