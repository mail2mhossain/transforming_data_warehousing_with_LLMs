from typing import Literal
from nodes.agent_state import AgentState
from nodes.nodes_name import QUERY_RELEVANCY_REPORT, RE_WRITE_QUERY

def make_decision_on_query_relevancy(state: AgentState) -> Literal[ RE_WRITE_QUERY, QUERY_RELEVANCY_REPORT ]:
    print("--- MAKE DECISION BASED ON QUERY RELEVANCY ---")

    is_query_relevant = state["is_query_relevant"]

    if is_query_relevant:
        return RE_WRITE_QUERY
    else:
        return QUERY_RELEVANCY_REPORT