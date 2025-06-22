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

# Blob Storage connection
blob_service_client = BlobServiceClient.from_connection_string(os.environ.get("BLOB_STORAGE_ENDPOINT"))

# --------------------------------------------------------
# 🔁 1. Blob Trigger – starts orchestration when a PDF is uploaded
# --------------------------------------------------------
@my_app.blob_trigger(arg_name="myblob", path="input", connection="BLOB_STORAGE_ENDPOINT")
@my_app.durable_client_input(client_name="client")
async def blob_trigger(myblob: func.InputStream, client):
    logging.info(f"Blob trigger processed: {myblob.name} ({myblob.length} bytes)")
    blobName = myblob.name.split("/")[-1]
    await client.start_new("process_document", client_input=blobName)

# --------------------------------------------------------
# 🧠 2. Orchestrator – controls sequence of actions
# --------------------------------------------------------
@my_app.orchestration_trigger(context_name="context")
def process_document(context):
    blobName = context.get_input()
    retry_opts = df.RetryOptions(first_retry_interval_in_milliseconds=5000, max_number_of_attempts=3)

    extracted_text = yield context.call_activity_with_retry("analyze_pdf", retry_opts, blobName)
    summary = yield context.call_activity_with_retry("summarize_text", retry_opts, extracted_text)
    output_file = yield context.call_activity_with_retry("write_doc", retry_opts, {
        "blobName": blobName,
        "summary": summary
    })

    logging.info(f"Summary saved as: {output_file}")
    return output_file

# --------------------------------------------------------
# 📄 3. analyze_pdf – extracts text using Azure Form Recognizer
# --------------------------------------------------------
@my_app.activity_trigger(input_name='blobName')
def analyze_pdf(blobName):
    logging.info("🔍 Starting PDF text extraction...")

    container_client = blob_service_client.get_container_client("input")
    blob_client = container_client.get_blob_client(blobName)
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

# --------------------------------------------------------
# ✨ 4. summarize_text – uses Azure OpenAI to summarize extracted text
# --------------------------------------------------------
@my_app.activity_trigger(input_name='results')
def summarize_text(results):
    logging.info("🧠 Summarizing text with Azure OpenAI...")

    client = openai.AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
            )

    response = client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),  # This is your deployment name, not model id
        messages=[
            {"role": "system", "content": "You are a helpful assistant that summarizes documents."},
            {"role": "user", "content": f"Summarize this:\n\n{results}"}
        ],
        temperature=0.5,
        max_tokens=512
    )

    return {"content": response.choices[0].message.content}

# --------------------------------------------------------
# 📤 5. write_doc – saves summary as a .txt file in 'output' container
# --------------------------------------------------------
@my_app.activity_trigger(input_name='results')
def write_doc(results):
    logging.info("💾 Writing summary to output container...")

    container_client = blob_service_client.get_container_client("output")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{results['blobName']}-{timestamp}.txt"

    container_client.upload_blob(name=file_name, data=results['summary']['content'], overwrite=True)
    return file_name

