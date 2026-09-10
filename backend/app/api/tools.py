from fastapi import APIRouter

from app.tools.registry import list_tool_catalog

router = APIRouter()


@router.get("/tools")
def get_tool_catalog():
    return list_tool_catalog()
