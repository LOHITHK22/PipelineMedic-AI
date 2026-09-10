"""LLM provider abstraction.

Only `mock` is guaranteed to work without network/API keys. `openai` and
`azure_openai` are real implementations but require credentials the demo
environment does not have; they are included for completeness and clean
extensibility, not exercised in CI/demo.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from app.models.schemas import DiagnosisResult, Incident, RepairPlan
from app.config import settings


class LLMProvider(ABC):
    @abstractmethod
    def diagnose(self, incident: Incident, context: dict) -> DiagnosisResult:
        ...

    @abstractmethod
    def generate_repair_plan(self, incident: Incident, diagnosis: DiagnosisResult, context: dict) -> RepairPlan:
        ...


class OpenAIProvider(LLMProvider):
    """Real OpenAI-backed provider using structured outputs (function-calling /
    response_format=json_schema). Requires OPENAI_API_KEY."""

    def __init__(self):
        import openai  # imported lazily so `mock` mode never needs the package installed

        if not settings.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY is required for llm_provider=openai")
        self.client = openai.OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model

    def _structured_call(self, system_prompt: str, user_prompt: str, schema_model):
        completion = self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format=schema_model,
        )
        return completion.choices[0].message.parsed

    def diagnose(self, incident: Incident, context: dict) -> DiagnosisResult:
        system = (
            "You are a pipeline reliability diagnostician. You NEVER invent facts not present "
            "in the evidence. You output structured diagnoses only."
        )
        user = f"Incident: {incident.model_dump_json()}\nContext: {context}"
        return self._structured_call(system, user, DiagnosisResult)

    def generate_repair_plan(self, incident: Incident, diagnosis: DiagnosisResult, context: dict) -> RepairPlan:
        system = (
            "You are a pipeline repair planner. You may only reference tools from the provided "
            "MCP tool catalog. Never propose shell commands or raw SQL. Output a structured plan."
        )
        user = f"Incident: {incident.model_dump_json()}\nDiagnosis: {diagnosis.model_dump_json()}\nContext: {context}"
        return self._structured_call(system, user, RepairPlan)


class AzureOpenAIProvider(OpenAIProvider):
    def __init__(self):
        import openai

        if not (settings.azure_openai_endpoint and settings.azure_openai_api_key and settings.azure_openai_deployment):
            raise RuntimeError("Azure OpenAI settings incomplete for llm_provider=azure_openai")
        self.client = openai.AzureOpenAI(
            azure_endpoint=settings.azure_openai_endpoint,
            api_key=settings.azure_openai_api_key,
            api_version="2024-08-01-preview",
        )
        self.model = settings.azure_openai_deployment


def get_llm_provider() -> LLMProvider:
    provider = settings.llm_provider.lower()
    if provider == "mock":
        from app.llm.mock_provider import MockLLMProvider

        return MockLLMProvider()
    if provider == "openai":
        return OpenAIProvider()
    if provider == "azure_openai":
        return AzureOpenAIProvider()
    raise ValueError(f"Unknown llm_provider: {provider}")
