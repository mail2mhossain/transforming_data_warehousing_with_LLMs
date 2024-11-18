import pandas as pd
from typing import Annotated, Sequence, TypedDict
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel


class AgentState(TypedDict):
    # messages: Annotated[list[AnyMessage], add_messages]
    query: str
    dataset_name: str
    db_path: str
    table_name: str
    column_descriptions: str
    sample_data: str
    SQL_query: str
    sanitize_check: int
    max_sanitize_check: int
    SQL_error: str
    sql_generation_try: int
    max_sql_generation_try: int
    data_frame: pd.DataFrame
    report_from_embeddings:str
    Python_Code: str
    Python_script_check: int
    execution_error: str
    max_Python_script_check: int
    script_security_issues:str
    execution_results: str
    Python_code_generated_report: str
    report_type: str
    reports: str