import { Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { Overview } from "./pages/Overview";
import { Incidents } from "./pages/Incidents";
import { IncidentDetail } from "./pages/IncidentDetail";
import { Approvals } from "./pages/Approvals";
import { AuditLog } from "./pages/AuditLog";
import { ApproveLink } from "./pages/ApproveLink";

export default function App() {
  return (
    <Routes>
      {/* Standalone, no main-dashboard chrome: meant to be landed on
          directly from a notification link, without necessarily having the
          rest of the dashboard "open" -- see docs/safety-model.md. */}
      <Route path="/approve-link/:token" element={<ApproveLink />} />
      <Route element={<Layout />}>
        <Route path="/" element={<Overview />} />
        <Route path="/incidents" element={<Incidents />} />
        <Route path="/incidents/:id" element={<IncidentDetail />} />
        <Route path="/approvals" element={<Approvals />} />
        <Route path="/audit" element={<AuditLog />} />
      </Route>
    </Routes>
  );
}
