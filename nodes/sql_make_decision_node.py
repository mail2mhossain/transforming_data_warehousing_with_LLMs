from langgraph.graph import END
from typing import Literal
from nodes.agent_state import AgentState
from nodes.nodes_name import (
    RE_GENERATE_SQL_QUERY,
    PYTHON_CODE_GENERATOR,
    REPORT_TYPE
)


def make_sql_decision(state:AgentState) -> Literal[ END, RE_GENERATE_SQL_QUERY, REPORT_TYPE ]:
    print("--- MAKING DECISION AFTER SQL EXECUTION ---")
    sql_generation_try = state['sql_generation_try']
    max_sql_generation_try = state['max_sql_generation_try']
    SQL_error = state['SQL_error']

    if SQL_error:
        if sql_generation_try >= max_sql_generation_try:
            return END
        else:
            sql_generation_try = sql_generation_try + 1
            state.update({
                "sql_generation_try": sql_generation_try,
            })
            print(f"SQL Generation Try: {sql_generation_try}")
            return RE_GENERATE_SQL_QUERY
    else:
        return REPORT_TYPE