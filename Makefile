.PHONY: up up-full down demo logs test test-mcp mcp-server mcp-inspector inject-schema-drift inject-poison inject-lag inject-airflow-failure reset-demo

up:
	docker compose up --build -d postgres kafka kafka-topic-init backend producer

up-full:
	docker compose --profile full up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f backend

test:
	cd backend && python -m pytest app/tests -v

test-mcp:
	cd backend && python -m pytest app/tests/test_mcp_server.py -v

# MCP server over stdio (see docs/mcp-tools.md). Read-only tools only unless
# MCP_ALLOW_STATE_CHANGING_TOOLS=true.
mcp-server:
	cd backend && python -m app.mcp_server

mcp-inspector:
	cd backend && npx -y @modelcontextprotocol/inspector@2.10.1 --config ../docs/mcp-inspector.example.json --server pipelinemedic

eval:
	cd backend && python -m app.eval.runner

inject-schema-drift:
	python scripts/inject_schema_drift.py

inject-poison:
	python scripts/inject_poison_message.py

inject-lag:
	python scripts/inject_lag.py

inject-airflow-failure:
	python scripts/inject_airflow_failure.py

reset-demo:
	python scripts/reset_demo.py

demo: up
	@echo "Waiting for backend to be ready..."
	sleep 15
	$(MAKE) inject-schema-drift
	@echo "Watch: curl http://localhost:8000/incidents"
