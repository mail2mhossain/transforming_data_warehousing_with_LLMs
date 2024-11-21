from langchain_core.output_parsers import StrOutputParser
from langgraph.graph import END
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from decouple import config
from nodes.agent_state import AgentState


REPHRASED_QUERY_PROMPT = """
    The user has provided the following query: {query}. 
    The query is relevant to the data. 
    The schema of the data is as follows: {df_columns}. 
    Rewrite the query to make it more specific, structured, and aligned with the schema provided. 
    Ensure the rewritten query is concise, uses plain English, and matches the data's context. 
    Provide only the rewritten query.
"""


def re_write_query(state: AgentState) -> AgentState:
    print("--- Re WRITING QUERY ---")

    OPENAI_API_KEY = config("OPENAI_API_KEY")
    GPT_MODEL = config("GPT_MODEL")

    query = state["query"]
    relevant_tables_and_columns = f"""
        Table Name: {state["table_name"]}
        Column Descriptions: {state["column_descriptions"]}
    """

    rephrased_query_prompt = PromptTemplate(
        template=REPHRASED_QUERY_PROMPT,
        input_variables=["query", "df_columns"],
    )

    model = ChatOpenAI(openai_api_key=OPENAI_API_KEY, temperature=0, model=GPT_MODEL, streaming=False, cache=False)

    rephrased_chain = rephrased_query_prompt | model | StrOutputParser()
    rephrased_question = rephrased_chain.invoke({"query": query, "df_columns": relevant_tables_and_columns})

    print(f"Rephrased Query: {rephrased_question}")
    return {
        "rephrased_query": rephrased_question,
    }