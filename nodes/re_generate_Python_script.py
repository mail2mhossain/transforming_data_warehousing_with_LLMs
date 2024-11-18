import json
import pandas as pd
from langchain.prompts.chat import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from langchain.schema import StrOutputParser
from langchain_openai import ChatOpenAI
from decouple import config
from nodes.agent_state import AgentState

sys_msg = """
    1. You are a Python expert. 
    """

user_msg = """
        1. You have a pandas dataframe in Python named `df`.
        2. The output of `print(df.head())` is: {df_head}.
        3. Use only the column names provided here: {df_columns}.
        4. When running the Python script (below), you get this error: 
        - **Error:** {execution_error}
        5. Here’s the script that causes the error:
        ```
        {Python_script}
        ```
        6. If there are any security issues in the script, address them: {security_issue}.
        7. Update the Python script based on the error, the security issue, and the column names.

        Please respond only with the corrected Python script, without extra explanation.

    """

def re_generate_Python_code(state: AgentState) -> AgentState:
    print("--- PYTHON CODE RE-GENERATOR ---")

    OPENAI_API_KEY = config("OPENAI_API_KEY")
    GPT_MODEL = config("GPT_MODEL")

    Python_script = state["Python_Code"]
    column_descriptions = state["column_descriptions"]
    description_dict = json.loads(column_descriptions)['columns'] # List object
    description_dict = {item['name']: item['description'] for item in description_dict}  # Dictionary
    df = state["data_frame"]
    script_security_issues = state.get("script_security_issues", "")
    execution_error = state.get("execution_error", "")
    Python_script_check = state['Python_script_check']

    df_head = str(df.head(5).to_markdown()) 

    prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessagePromptTemplate.from_template(sys_msg),
            HumanMessagePromptTemplate.from_template(user_msg),
        ]
    )

    llm = ChatOpenAI(model_name=GPT_MODEL, temperature=0, openai_api_key=OPENAI_API_KEY)
    chain = prompt | llm | StrOutputParser()
    code = chain.invoke({"df_head": df_head, 
                         "df_columns": str(description_dict), 
                         "Python_script": Python_script, 
                         "security_issue": script_security_issues, 
                         "execution_error": execution_error})

    print(f"Re-generated Python code:\n{code}")
    return {
        "Python_Code" : code,
        "Python_script_check": Python_script_check + 1
    }