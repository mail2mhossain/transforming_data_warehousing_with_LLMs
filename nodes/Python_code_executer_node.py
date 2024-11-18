import pandas as pd
from langgraph.graph import END
from langchain_experimental.tools.python.tool import PythonAstREPLTool, PythonREPLTool
from nodes.agent_state import AgentState

def run_python_code(state: AgentState) -> AgentState:
    print("--- PYTHON CODE EXECUTER ---")
    Python_script = state['Python_Code']
    
    df = state["data_frame"]
    df_locals={}
    df_locals["df"] = df
    python_repl = PythonAstREPLTool(locals=df_locals)
    
    try: 
        results = python_repl.run(Python_script)     
        if "error:" in results.lower():
            return {
                "execution_error": results
            }
        else:
            print(f"Execution Results:\n{results}")
            return {
                "execution_results": results,
                "execution_error": None
            }
    except Exception as e:
        return {
            "execution_error": str(e)
        }


    