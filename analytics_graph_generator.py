
from langgraph.graph import START, StateGraph, END
from langgraph.graph.graph import CompiledGraph
from nodes.query_relevancy_check_node import check_query_relevancy
from nodes.make_decision_based_on_query_relevant_node import make_decision_on_query_relevancy
from nodes.query_relevancy_report_node import query_relevancy_report
from nodes.query_re_write_node import re_write_query
from nodes.get_dataset_detail_node import get_dataset_detail
from nodes.sql_query_generation_node import generate_sql_query
from nodes.sql_query_sanitize_node import sanitize_sql_query
from nodes.sql_query_executer_node import execute_sql_query
from nodes.sql_query_error_report_node import sql_query_error_report
from nodes.sql_query_regeneration_node import regenerate_sql_query
from nodes.sql_make_decision_node import make_sql_decision
from nodes.Python_code_generator_node import generate_Python_code
from nodes.Python_code_sanitize_node import sanitize_python_script
from nodes.Python_code_executer_node import run_python_code
from nodes.re_generate_Python_script import re_generate_Python_code
from nodes.report_generator_node import generate_report
from nodes.make_decision_node import make_decision
from nodes.generate_report_type_node import get_report_type
from nodes.agent_state import AgentState 
from nodes.nodes_name import (
    QUERY_RELEVANCY_CHECKER, 
    QUERY_RELEVANCY_REPORT,
    RE_WRITE_QUERY,
    DATASET_DETAILS,
    QUERY_GENERATION,
    EXECUTE_SQL_QUERY,
    SQL_QUERY_EXECUTION_ERROR_REPORT,
    RE_GENERATE_SQL_QUERY,
    PYTHON_CODE_GENERATOR,
    PYTHON_CODE_EXECUTER,
    PYTHON_CODE_RE_GENERATION,
    REPORT_GENERATOR,
    REPORT_TYPE,
)

def generate_graph()-> CompiledGraph:
    workflow = StateGraph(AgentState)
    workflow.add_node(QUERY_RELEVANCY_CHECKER, check_query_relevancy)
    workflow.add_node(QUERY_RELEVANCY_REPORT, query_relevancy_report)
    workflow.add_node(RE_WRITE_QUERY, re_write_query)

    workflow.add_node(DATASET_DETAILS, get_dataset_detail)

    workflow.add_node(QUERY_GENERATION, generate_sql_query)
    workflow.add_node(EXECUTE_SQL_QUERY, execute_sql_query)
    workflow.add_node(SQL_QUERY_EXECUTION_ERROR_REPORT, sql_query_error_report)
    workflow.add_node(RE_GENERATE_SQL_QUERY, regenerate_sql_query)

    workflow.add_node(REPORT_TYPE, get_report_type)

    workflow.add_node(PYTHON_CODE_GENERATOR, generate_Python_code)
    workflow.add_node(PYTHON_CODE_EXECUTER, run_python_code)
    workflow.add_node(PYTHON_CODE_RE_GENERATION, re_generate_Python_code)

    workflow.add_node(REPORT_GENERATOR, generate_report)


    workflow.add_edge(START, DATASET_DETAILS)
    workflow.add_edge(DATASET_DETAILS, QUERY_RELEVANCY_CHECKER)
    
    workflow.add_conditional_edges(
        QUERY_RELEVANCY_CHECKER,
        make_decision_on_query_relevancy,
    )

    workflow.add_edge(RE_WRITE_QUERY, QUERY_GENERATION)

    workflow.add_conditional_edges(
        QUERY_GENERATION,
        sanitize_sql_query,
    )

    workflow.add_conditional_edges(
        EXECUTE_SQL_QUERY,
        make_sql_decision,
    )

    workflow.add_conditional_edges(
        RE_GENERATE_SQL_QUERY,
        sanitize_sql_query
    )

    workflow.add_edge(REPORT_TYPE, PYTHON_CODE_GENERATOR)

    workflow.add_conditional_edges(
        PYTHON_CODE_GENERATOR,
        sanitize_python_script,
    )
    workflow.add_conditional_edges(
        PYTHON_CODE_EXECUTER,
        make_decision,
    )

    workflow.add_conditional_edges(
        PYTHON_CODE_RE_GENERATION,
        sanitize_python_script,
    )

    workflow.add_edge(QUERY_RELEVANCY_REPORT, END)
    workflow.add_edge(SQL_QUERY_EXECUTION_ERROR_REPORT, END)
    workflow.add_edge(REPORT_GENERATOR, END)

    graph = workflow.compile()
    
    graph.get_graph(xray=1).draw_mermaid_png(output_file_path="data_analytics_graph.png")

    return graph


def get_reports(dataset_name, query):
    app = generate_graph()

    results = app.invoke({"dataset_name": dataset_name, 
                "query": query, 
                "sanitize_check": 0,
                "max_sanitize_check": 5,
                "sql_generation_try": 0,
                "max_sql_generation_try": 5,
                'Python_script_check': 0,
                'max_Python_script_check': 5,
                })

   
    return results.get("reports", "No reports found")

# reports = get_reports("Green Trip Data", "Average Trip Distance by Payment Type")
# print(reports)