import pandas as pd
from typing import Annotated, Sequence, TypedDict
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel


class DBState(TypedDict):
    dataset_name: str
    column_descriptions: str
    data_frame: pd.DataFrame
    db_name: str
    table_name: str
    db_creation_error: str
   