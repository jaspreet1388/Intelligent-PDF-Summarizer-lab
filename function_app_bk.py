import logging
import os
import json
from datetime import datetime

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
import openai

# Initialize Durable Function App
my_app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(os.environ.get("BLOB_STORAGE_ENDPOINT"))

# Blob Trigger Function
@my_app.blob_trigger(arg_name="myblob", path="input", connection="BLOB_STORAGE_ENDPOINT")
@my_app.durable_client_input(client_name="client")
async def blob_trigger(myblob: func.InputStream, client):
    logging.info(f"Python blob trigger function processed blob\n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    blob_name = myblob.name.split("/")[1]
    await client.start_new("process_document", client_input=blob_name)

# Orchestrator Function
@my_app.orchestration_trigger(context_name="context")
def process_document(context):
    blob_name: str = context.get_input()

    retry_options = df.RetryOptions(first_retry_interval_in_milliseconds=5000, max_number_of_attempts=3)

    result = yield context.call_activity_with_retry("analyze_pdf", retry_options, blob_name)
    result2 = yield context.call_activity_with_retry("summarize_text", retry_options, result)
    result3 = yield context.call_activity_with_retry("write_doc", retry_options, {
        "blobName": blob_name,
        "summary": result2
    })

    logging.info(f"Successfully uploaded summary to {result3}")
    return result3

# PDF Analyzer Activity
@my_app.activity_trigger(input_name='blobName')
def analyze_pdf(blob_name):
    logging.info("Starting PDF analysis...")

    container_client = blob_service_client.get_container_client("input")
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob().readall()

    endpoint = os.environ["FORM_RECOGNIZER_ENDPOINT"]
    key = os.environ["FORM_RECOGNIZER_KEY"]
    client = DocumentAnalysisClient(endpoint, AzureKeyCredential(key))

    poller = client.begin_analyze_document("prebuilt-layout", document=blob_data, locale="en-US")
    result = poller.result().pages

    extracted_text = ""
    for page in result:
        for line in page.lines:
            extracted_text += line.content + " "

    return extracted_text

# Summarization Activity
@my_app.activity_trigger(input_name='results')
def summarize_text(results):
    logging.info("Generating summary using OpenAI...")

    openai.api_key = os.getenv("OPENAI_API_KEY")

    response = openai.ChatCompletion.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that summarizes documents."},
            {"role": "user", "content": f"Summarize this:\n\n{results}"}
        ],
        temperature=0.5,
        max_tokens=512
    )

    return {"content": response["choices"][0]["message"]["content"]}

# Output Writer Activity
@my_app.activity_trigger(input_name='results')
def write_doc(results):
    logging.info("Uploading summary to output container...")

    container_client = blob_service_client.get_container_client("output")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{results['blobName']}-{timestamp}.txt"
    sanitized_name = file_name.replace(" ", "_")

    container_client.upload_blob(name=sanitized_name, data=results['summary']['content'], overwrite=True)
    return sanitized_name

