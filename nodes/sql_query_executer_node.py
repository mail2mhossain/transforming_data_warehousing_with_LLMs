import duckdb
import pandas as pd
from langchain_core.messages import FunctionMessage
from nodes.agent_state import AgentState


def execute_sql_query(state: AgentState) -> AgentState:
    print("--- EXECUTE SQL QUERY ---")
    
    db_path = state["db_path"]
    sql_query = state["SQL_query"]
        
    conn_persistent = duckdb.connect(db_path)
    try:

        df = conn_persistent.execute(sql_query).fetchdf()
        print(f"Data Frame from SQL:\n{df}")
        
        return {
            "data_frame": df,
            "SQL_error": None,
        }
    
    except Exception as e:
        print(f"SQL Execution Error: {e}")
        return {
            "data_frame": None,
            "SQL_error": str(e),
            "sanitize_check": 0
        }
    
    finally:
        conn_persistent.close()
    
    


# results = execute_sql_query({"SQL_query": "SELECT * FROM public.actor LIMIT 10"})

# if results["data_frame"] is not None:
#     print(results["data_frame"])
# else:
#     print(results["SQL_error"])