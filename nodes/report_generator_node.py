import json
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
You are a report generator expert. Combine numerical analysis with narrative descriptions, visualizations and charts. Generate report in markdown format only.
"""

user_msg = """
    Say, I have a data frame as following:
    {df}
    DO NOT USE ENUM OR NUMERIC REPRESENTATION OF A COLUMN IN REPORTS, ALWAYS USE COLUMN DESCRIPTION. Column Descriptions: {column_descriptions}
    User asked a question: {query}
    Write a report on user's query in professional tone using plain and simple English. 
    Combine numerical analysis with narrative descriptions, visualizations and charts.
    Report should reflect only the answer of the user's query based on {execution_results} in Markdown format.
    DO NOT INCLUDE ```markdown TAGS IN YOUR RESPONSE.
    Do not include any other text or your assumptions in the report.
    """

def generate_report(state: AgentState) -> AgentState:
    print("--- REPORT GENERATOR ---")

    OPENAI_API_KEY = config("OPENAI_API_KEY")
    GPT_MODEL = config("GPT_MODEL")

    rephrased_query = state["rephrased_query"]
    df = state["data_frame"]
    column_descriptions = state["column_descriptions"]
    description_dict = json.loads(column_descriptions)['columns'] # List object
    description_dict = {item['name']: item['description'] for item in description_dict}  # Dictionary
    execution_results = state["execution_results"]

    prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessagePromptTemplate.from_template(sys_msg),
            HumanMessagePromptTemplate.from_template(user_msg),
        ]
    )
    llm = ChatOpenAI(model_name=GPT_MODEL, temperature=0, openai_api_key=OPENAI_API_KEY)
    chain = prompt | llm | StrOutputParser()
    reports = chain.invoke({"query": rephrased_query, 
                            "column_descriptions": str(description_dict),
                            "execution_results": execution_results, 
                            "df": str(df)})

    with open("reports.md", "w") as file:
        file.write(reports)

    return {
        "reports": reports
    }