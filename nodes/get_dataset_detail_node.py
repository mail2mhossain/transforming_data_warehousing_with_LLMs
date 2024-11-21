from nodes.file_manager_db import get_db_info_by_dataset
from nodes.agent_state import AgentState 

def get_dataset_detail(state: AgentState) -> AgentState:
    print("--- GET DATASET DETAIL ---")
    dataset_name = state["dataset_name"]
    db_info = get_db_info_by_dataset(dataset_name)

    print(f"Table Name: {db_info.table_name}")
    return {
        "db_path": db_info.duck_db_path,
        "table_name": db_info.table_name,
        "column_descriptions": db_info.column_descriptions,
        "sample_data": db_info.df_head
    }