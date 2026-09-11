from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.health import router as health_router
from app.api.incidents import router as incidents_router
from app.api.tools import router as tools_router
from app.config import settings
from app.db.base import Base, apply_lightweight_schema_patches, engine
from app.observability.logging_config import configure_logging
from app.services.pipeline_monitor import monitor

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    apply_lightweight_schema_patches()
    if settings.app_env != "test":
        monitor.start()
    yield
    monitor.stop()


app = FastAPI(title="PipelineMedic AI", version="0.1.0", lifespan=lifespan)

# NOTE: permissive CORS here is for local development only, so a separately
# served React dashboard (Vite dev server / static container) can call this
# API from a different origin. This is NOT appropriate for production and is
# intentionally left out of the "production boundary" hardening doc.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(incidents_router, tags=["incidents"])
app.include_router(tools_router, tags=["tools"])
app.include_router(health_router, tags=["health"])


@app.get("/health")
def health():
    return {"status": "ok", "llm_provider": settings.llm_provider}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/", response_class=HTMLResponse)
def status_page():
    from app.db.base import SessionLocal
    from app.db.models import Incident as IncidentRow

    db = SessionLocal()
    try:
        incidents = db.query(IncidentRow).order_by(IncidentRow.created_at.desc()).limit(50).all()
    finally:
        db.close()

    rows_html = "".join(
        f"<tr><td>{i.id[:8]}</td><td>{i.incident_type}</td><td>{i.severity}</td>"
        f"<td><b>{i.status.value}</b></td><td>{i.title}</td><td>{i.created_at}</td></tr>"
        for i in incidents
    )
    return f"""
    <html><head><title>PipelineMedic AI</title>
    <style>
      body {{ font-family: system-ui, sans-serif; margin: 2rem; background:#0b0f14; color:#e6edf3; }}
      table {{ border-collapse: collapse; width: 100%; }}
      td, th {{ border: 1px solid #30363d; padding: 6px 10px; text-align:left; font-size: 14px; }}
      th {{ background: #161b22; }}
      h1 {{ color:#58a6ff; }}
    </style></head>
    <body>
      <h1>PipelineMedic AI — Status</h1>
      <p>LLM provider: <b>{settings.llm_provider}</b> | <a style="color:#58a6ff" href="/metrics">/metrics</a> | <a style="color:#58a6ff" href="/incidents">/incidents (JSON)</a></p>
      <table>
        <tr><th>ID</th><th>Type</th><th>Severity</th><th>Status</th><th>Title</th><th>Created</th></tr>
        {rows_html or '<tr><td colspan="6">No incidents yet — run a fault-injection script.</td></tr>'}
      </table>
    </body></html>
    """
