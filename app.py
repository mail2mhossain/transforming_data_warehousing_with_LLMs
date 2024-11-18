# streamlit run your_script.py

import time
import os
import io
import re
import json
import uuid 
import zipfile
import threading
from queue import Queue, LifoQueue
from pathlib import Path
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from nodes.file_manager_db import if_dataset_exist, update_column_description, is_file_loaded, get_all_file_info
from nodes.load_new_data_node import load_new_data
from db_graph_generator import execute_graph
from analytics_graph_generator import get_reports
from dotenv import load_dotenv, set_key

st.cache_data.clear()

load_dotenv()

data_directory = os.path.join(os.path.dirname(__file__), "data")


# Initialize session states
if 'history' not in st.session_state:
    st.session_state['history'] = []
if 'last_report' not in st.session_state:
    st.session_state['last_report'] = None
if 'query' not in st.session_state:
    st.session_state['query'] = ''
if 'selected_dataset_name' not in st.session_state:
    st.session_state['selected_dataset_name'] = None
if 'selected_dataset_name_4_new_data' not in st.session_state:
    st.session_state['selected_dataset_name_4_new_data'] = None
if 'edited_columns' not in st.session_state:
    st.session_state['edited_columns'] = None
if 'form_submitted' not in st.session_state:
    st.session_state['form_submitted'] = False
if 'data_frame' not in st.session_state:
    st.session_state['data_frame'] = None  # Store the DataFrame directly in session state
if 'openai_api_key' not in st.session_state:
    st.session_state['openai_api_key'] = os.getenv("OPENAI_API_KEY", "")
if 'gpt_model' not in st.session_state:
    st.session_state['gpt_model'] = os.getenv("GPT_MODEL", None)
if "result_queue" not in st.session_state:
    st.session_state.result_queue = LifoQueue()
if "embedding_thread" not in st.session_state:
    st.session_state.embedding_thread = None
if "embedding_complete" not in st.session_state:
    st.session_state.embedding_complete = False
if "progress" not in st.session_state:
    st.session_state["progress"] = 0.0
if "progress_caption" not in st.session_state:
    st.session_state["progress_caption"] = ""


def update_env_variable(key, value):
    env_path = ".env"
    set_key(env_path, key, value)

def sanitize_filename(query):
    # Remove special characters and limit filename length
    sanitized = re.sub(r'[^a-zA-Z0-9_\- ]', '', query)  # Allow only alphanumeric, underscore, hyphen, and space
    sanitized = "_".join(sanitized.split())  # Replace spaces with underscores
    return sanitized[:50]  # Limit to 50 characters for readability

def download_png_files(png_paths):
    png_files = {}
    for path in png_paths:
        try:
            with open(path, 'rb') as f:
                png_files[Path(path).name] = f.read()
        except FileNotFoundError:
            st.warning(f"File {path} not found")
            
    return png_files

def download_reports(query, report):
    markdown_content = f"# Query: {query}\n\n{report}"
    markdown_file = io.StringIO(markdown_content)  # Use StringIO to create an in-memory file
    filename = sanitize_filename(query) + ".md"
    download_key = str(uuid.uuid4())
    st.download_button(
        label="Download Report",
        data=markdown_file.getvalue(),
        file_name=filename,
        mime="text/markdown",
        key=download_key
    )

def download_reports_with_png(query, report):
    # Extract PNG file paths from the markdown report
    pattern = r'!\[.*?\]\((.*?\.png)\)'
    png_paths = re.findall(pattern, report)
    
    # Download or collect PNG files
    png_files = download_png_files(png_paths)

    # Create a ZIP file in-memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        # Add markdown report to the ZIP
        markdown_content = f"# Query: {query}\n\n{report}"
        markdown_filename = sanitize_filename(query) + ".md"
        zip_file.writestr(markdown_filename, markdown_content)

        # Add PNG files to the ZIP
        for file_name, content in png_files.items():
            zip_file.writestr(f"images/{file_name}", content)

    zip_buffer.seek(0)  # Move the buffer pointer to the start

    # Offer the ZIP file as a download
    zip_filename = sanitize_filename(query) + ".zip"
    download_key = str(uuid.uuid4())
    st.download_button(
        label="Download Report",
        data=zip_buffer,
        file_name=zip_filename,
        mime="application/zip",
        key=download_key
    )

def update_headings(text):
    # Replace headings in the order from largest to smallest to prevent conflicts
    text = re.sub(r'(?m)^###### ', '######## ', text)  # Handle heading 6, if any
    text = re.sub(r'(?m)^##### ', '####### ', text)
    text = re.sub(r'(?m)^#### ', '###### ', text)
    text = re.sub(r'(?m)^### ', '##### ', text)
    text = re.sub(r'(?m)^## ', '#### ', text)
    text = re.sub(r'(?m)^# ', '### ', text)
    return text

def display_reports(markdown_text):
    # Regex to find image references and capture captions and paths
    image_pattern = r'!\[(.*?)\]\((.*?)\)'

    # Split the markdown text into segments by images
    parts = re.split(image_pattern, markdown_text)

    # Find all image references to extract their captions and paths
    image_matches = re.findall(image_pattern, markdown_text)

    # Display the content and images alternately
    text_index = 0

    if len(image_matches) == 0:
        # No images found, display the full markdown text at once
        st.markdown(markdown_text)
    else:
        # Proceed with the original loop logic if there are images
        for i in range(len(parts)):
            if i % 3 == 0:
                st.markdown(parts[i])
            elif i % 3 == 1:
                if text_index < len(image_matches):
                    caption, path = image_matches[text_index]
                    if os.path.exists(path):
                        st.image(path.strip(), caption=caption)
                    text_index += 1

def submit_query():
    if st.session_state['last_report']:
        st.session_state['history'].append(st.session_state['last_report'])

    selected_name = st.session_state['selected_dataset_name']
    if selected_name:
        if if_dataset_exist(selected_name):
            try:
                with st.spinner("Generating report..."):
                    st.session_state['query'] = st.session_state['query_input']
                    report = get_reports(selected_name, st.session_state['query'])
                    report = update_headings(report)
                    st.session_state['last_report'] = (st.session_state['query'], report)
            except Exception as e:
                st.error(f"Error occurred: {e}")
        else:
            st.error("Selected dataset not found.")
    else:
        st.warning("Please select a dataset.")

    st.session_state['query'] = ''

# Title of the app
st.title("Data Analytics")
    

def data_analysis_content():
    st.subheader("Select Dataset for Analysis")
    
    if not all([st.session_state['openai_api_key'], st.session_state['gpt_model']]):
        st.info("First Configure OpenAI API Key and Model Name in the 'Configuration' tab. After that, configure Dataset and upload data.")
    else:
        available_datasets = get_all_file_info()
        if available_datasets:
            dataset_choices = [dataset["name"] for dataset in available_datasets]
            st.selectbox("Choose a dataset:", dataset_choices, key="selected_dataset_name", index=None)
        else:
            st.info("No datasets available. Please configure Dataset and upload data.")

    if st.session_state['selected_dataset_name']:
        with st.form(key='query_form'):
            query = st.text_input("Enter your query:", value=st.session_state['query'], key="query_input")
            submit_button = st.form_submit_button(label="Generate Report", on_click=submit_query)

        if st.session_state['last_report']:
            query, report = st.session_state['last_report']
            st.markdown(f"### Query: {query}")
            display_reports(report)
            download_reports_with_png(query, report)

        st.subheader("Chat History")
        total_history = len(st.session_state['history']) 
        for i, (query, report) in enumerate(reversed(st.session_state['history'])):
            st.markdown(f"### Query {total_history - i}: {query}")
            display_reports(report)
            download_reports_with_png(query, report)
    else:
        st.info("Please select a dataset for analysis.")

def get_temp_file(uploaded_file):
    temp_file_path = os.path.join("temp", uploaded_file.name)
    os.makedirs("temp", exist_ok=True)  
    with open(temp_file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return temp_file_path

def display_column_descriptions(dataset_name, temp_file_path):
    with st.spinner("Display Column Descriptions..."):
        results = execute_graph(dataset_name, temp_file_path)
        # print(results["db_creation_error"])
        db_creation_result = json.loads(results["db_creation_error"])
        if db_creation_result['success']:
            json_data = json.loads(results["column_descriptions"])
            st.session_state.edited_columns = [{"name": col["name"], "description": col["description"]} for col in json_data["columns"]]
            st.session_state.form_submitted = False

            st.session_state['data_frame'] = results["data_frame"]

    return db_creation_result

def edit_descriptions(key):
    # Form for editing column descriptions
    with st.form("edit_columns_form"+key):
        st.write("Modify the Column Descriptions (**Data Dictionary**)")
        for i, column in enumerate(st.session_state.edited_columns):
            description = st.text_area(
                f"Description for '{column['name']}'",
                value=column["description"],
                key=f"description_{i}_{key}" 
            )
            st.session_state.edited_columns[i]["description"] = description

        submitted = st.form_submit_button("Save Changes")
        if submitted:
            st.session_state.form_submitted = True
            print(f"Save button is pressed. {st.session_state.form_submitted}")
            
def configure_dataset():
    st.subheader("Configure Datasets")

    st.session_state.edited_columns = None
    st.session_state.form_submitted = False
    st.session_state['data_frame'] = None

    uploaded_file = st.file_uploader("Upload a parquet file", type="parquet", key="configuration_dataset")
    dataset_name = st.text_input("Dataset Name")

    if st.button("Configure Dataset"):
        if uploaded_file and dataset_name:
            try:
                temp_file_path = get_temp_file(uploaded_file)
                with st.spinner("Configuring Dataset..."):
                    db_creation_result = display_column_descriptions(dataset_name, temp_file_path)
                    if db_creation_result['success']:
                        st.success(db_creation_result['message'])
                    else:
                        st.error(db_creation_result['message'])
                    
                    # Clean up temp file
                    os.remove(temp_file_path)
            except Exception as e:
                st.error(f"Error in Configuring Dataset: {e}")
        else:
            st.warning("Please upload a parquet file and provide a dataset name.")

    # Display data if loaded in session state
    if st.session_state.get('data_frame') is not None and st.session_state.get('edited_columns'):
        st.dataframe(st.session_state['data_frame'].sample(10))
        
        edit_descriptions("config")

        if st.session_state.form_submitted:
            # Convert edited data to JSON string format
            updated_json_data = json.dumps({"columns": st.session_state.edited_columns})
            # Call the update function
            update_result = update_column_description(dataset_name, updated_json_data)
            update_result = json.loads(update_result)
            # Reset submission flag to prevent repeated calls on rerun
            st.session_state.form_submitted = False
            if update_result["success"]:
                st.success("Column description updated successfully!")
            else:
                st.error(update_result["message"])

# @st.fragment(run_every="60s")
def auto_function(dataset_name_4_upload_new_data, temp_file_path, progress_caption, progress_bar):
    file_loading_message = is_file_loaded(dataset_name_4_upload_new_data, temp_file_path)
    
    progress_value = file_loading_message['progress']
    
    st.session_state["progress"] = progress_value
    caption = f"Embedding generation progress: {progress_value * 100:.2f}%"

    progress_caption.caption(caption)
    progress_bar.progress(progress_value)

def update_progress_value(value):
    st.session_state["progress"] = value
    st.session_state["progress_caption"] = f"Embedding generation progress: {value * 100:.2f}%"
    print(f"{st.session_state['progress_caption']}")
    
def load_data_in_background(dataset_name, parquet_file_path, offset, progress):   
    if offset > 0:
        update_progress_value(progress)

    def update_progress(value):
        update_progress_value(value)

    thread = threading.Thread(target=load_new_data, args=(dataset_name, parquet_file_path, offset, update_progress))
    thread.start()

    return thread

def upload_new_data():
    st.subheader("Upload New Data")
    st.session_state.edited_columns = None
    st.session_state.form_submitted = False
    st.session_state['data_frame'] = None

    if not all([st.session_state['openai_api_key'], st.session_state['gpt_model']]):
        st.info("First Configure OpenAI API Key and Model Name in the 'Configuration' tab. After that, configure Dataset and upload data.")
    else:
        available_datasets = get_all_file_info()
        if available_datasets:
            dataset_choices = [dataset["name"] for dataset in available_datasets]
            st.selectbox("Choose a dataset:", dataset_choices, key="selected_dataset_name_4_new_data", index=None)
        else:
            st.info("No datasets available. Please configure Dataset and upload data.")

    
    if st.session_state['selected_dataset_name_4_new_data']:
        dataset_name_4_upload_new_data = st.session_state['selected_dataset_name_4_new_data']
        uploaded_file = st.file_uploader("Upload a parquet file", type="parquet", key="upload_new_data")
        if uploaded_file and dataset_name_4_upload_new_data:
            temp_file_path = get_temp_file(uploaded_file)
            if st.button("Upload New Data"):
                file_loading_message = is_file_loaded(dataset_name_4_upload_new_data, temp_file_path)
                # file_loading_message = json.loads(file_loading_message)
                if file_loading_message['success']:
                    st.success(file_loading_message['message'])
                else:
                    message = load_new_data(dataset_name_4_upload_new_data, temp_file_path)
                    if message["success"]:
                        st.success(message["message"])

                    # thread = load_data_in_background(dataset_name_4_upload_new_data, temp_file_path, file_loading_message['offset'], file_loading_message['progress'] )
                    
                    # progress_caption = st.caption(st.session_state["progress_caption"])
                    # progress_bar = st.progress(st.session_state["progress"])

                    # st.write("Embeddings generation started in the background. Please wait...") # You can continue using the app.

                    # while thread.is_alive():
                    #     auto_function(dataset_name_4_upload_new_data, temp_file_path, progress_caption, progress_bar)
                    #     time.sleep(30)
                    # else:
                    #     auto_function(dataset_name_4_upload_new_data, temp_file_path, progress_caption, progress_bar)
                    #     st.success("Data loading complete!")

            display_column_descriptions(dataset_name_4_upload_new_data, temp_file_path)

    if st.session_state.get('data_frame') is not None and st.session_state.get('edited_columns'):
        st.dataframe(st.session_state['data_frame'].sample(10))
        
        edit_descriptions("new_data")

        if st.session_state.form_submitted:
            # Convert edited data to JSON string format
            updated_json_data = json.dumps({"columns": st.session_state.edited_columns})
            # Call the update function
            update_result = update_column_description(dataset_name_4_upload_new_data, updated_json_data)
            update_result = json.loads(update_result)
            # Reset submission flag to prevent repeated calls on rerun
            st.session_state.form_submitted = False
            if update_result["success"]:
                st.success("Column description updated successfully!")
            else:
                st.error(update_result["message"])

def configuration_content():
    st.subheader("Configuration")

    # Input fields for API key and model name
    api_key_input = st.text_input("OpenAI API Key", value=st.session_state['openai_api_key'], type="password")
    model_options = ["gpt-3.5-turbo", "gpt-4o", "gpt-4o-mini", "gpt-4"]
    model_name_input = st.selectbox("OpenAI Model Name", options=model_options, index=model_options.index(st.session_state['gpt_model']) if st.session_state['gpt_model'] in model_options else None)
    def save_configuration():
        # Update session state with the new configuration values
        st.session_state['openai_api_key'] = api_key_input
        st.session_state['gpt_model'] = model_name_input
        update_env_variable("OPENAI_API_KEY", api_key_input)
        update_env_variable("GPT_MODEL", model_name_input)
        load_dotenv()
        st.success("Configuration saved successfully!")

    # Save configuration button with the callback function
    st.button("Save Configuration", on_click=save_configuration)


# Tab structure
tab1, tab2, tab3, tab4 = st.tabs(["Data Analysis", "Configure Dataset", "Upload New Data", "Configuration"])

with tab1:
    data_analysis_content()
with tab2:
    configure_dataset()
with tab3:
    upload_new_data()
with tab4:
    configuration_content()

    
