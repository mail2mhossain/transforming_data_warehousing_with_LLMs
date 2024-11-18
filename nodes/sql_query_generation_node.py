from langchain.prompts.chat import ChatPromptTemplate
from nodes.agent_state import AgentState
from langchain_openai import ChatOpenAI
from langchain.schema import StrOutputParser
from decouple import config


sys_msg = """
    You are an expert SQL query generator with deep knowledge of DuckDB syntax and best practices. 
    You are given a DuckDB database schema which contains relevant tables and columns, referred to as `relevant_tables_and_columns`. 
    Additionally, you will receive a natural language user query, referred to as `query`, that needs to be transformed into a valid, efficient, 
    and correct DuckDB SQL query. 

    You are also provided sample data, referred as 'sample_data':

    Your task is to:
    1. Analyze the given 'domain_specific_terms' and `relevant_tables_and_columns` structure to identify the appropriate tables and columns needed for the query.
    2. Interpret the user's query and map it to the correct SQL operations (e.g., SELECT, JOIN, WHERE, GROUP BY, etc.).
    3. Ensure that the SQL query is optimized, adheres to DuckDB standards, and accurately represents the intent of the user's query.
    4. Handle edge cases such as ambiguous column names, missing conditions, or complex queries by making reasonable assumptions or asking for clarification when needed.
    5. DO NOT INCLUDE ```sql``` TAGS IN YOUR RESPONSE.
    Your response should consist solely of the generated SQL query in correct syntax, without any additional explanation.
"""

def generate_sql_query(state:AgentState)->AgentState:
    print("--- GENERATE SQL QUERY ---")
    
    OPENAI_API_KEY = config("OPENAI_API_KEY")
    GPT_MODEL = config("GPT_MODEL")

    llm = ChatOpenAI(model_name=GPT_MODEL, temperature=0, openai_api_key=OPENAI_API_KEY)

    prompt = ChatPromptTemplate(
        [
            ("system", sys_msg),
            ("human", """
                relevant_tables_and_columns: {relevant_tables_and_columns}
                sample_data: {sample_data}
                query: {query}
            """)
        ]
    )

    relevant_tables_and_columns = f"""
        Table Name: {state["table_name"]}
        Column Descriptions: {state["column_descriptions"]}
    """

    chain = prompt | llm | StrOutputParser()
    response = chain.invoke({
        "relevant_tables_and_columns": relevant_tables_and_columns, 
        "sample_data": state["sample_data"],
        "query": state["query"]
    })
    print(f"Generated SQL query: {response}")
    return {
        "SQL_query": response
    }