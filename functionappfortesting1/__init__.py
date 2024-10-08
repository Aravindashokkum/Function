import azure.functions as func
import pandas as pd
import json
import os
import base64
from io import BytesIO
import requests
import logging
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Environment variables
connection_string = "DefaultEndpointsProtocol=https;AccountName=storageappfortesting;AccountKey=g6UkOhk//Fqgg9sx54ja0tbH2i/jlAUgbYOcJAU29fC1aQAIprk9HRryxX9JosM/xUG6Jr9EP+6H+AStgtwIUg==;EndpointSuffix=core.windows.net"
container_name = "new"
blob_name = "Databricks-partner-course-status.xlsx"

GITHUB_TOKEN = "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"
REPO_OWNER = "Aravindashokkum"
REPO_NAME = "function"
BRANCH_NAME = "main"
json_filename = "tfvars.file"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Step 1: Download Excel file from Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Download the Excel file into memory
        excel_data = blob_client.download_blob().readall()
        df = pd.read_excel(BytesIO(excel_data))
        logging.info(f"Downloaded and read {blob_name} from Azure Blob Storage.")

        # Step 2: Convert Excel to JSON
        json_data = df.to_dict(orient='records')
        json_string = json.dumps(json_data, indent=4)
        logging.info(f"Data prepared for {json_filename}.")

        # Properly encode the JSON string in Base64
        encoded_content = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')

        # Step 3: Push JSON to GitHub
        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{json_filename}"

        # Get the SHA of the file if it exists (for updating)
        response = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
        sha = response.json().get('sha') if response.status_code == 200 else None

        # Prepare the request data
        data = {
            "message": "Update output.json with latest data",
            "content": encoded_content,  # Use the Base64 encoded content
            "branch": BRANCH_NAME,
        }

        if sha:
            data["sha"] = sha  # Include SHA for updating the existing file

        # Create or update the file in the GitHub repository
        response = requests.put(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, json=data)

        if response.status_code in (201, 200):
            logging.info("Changes pushed to GitHub.")
            return func.HttpResponse("Data processed and pushed to GitHub successfully.", status_code=200)
        else:
            logging.error(f"Error pushing to GitHub: {response.text}")
            return func.HttpResponse(f"Error pushing to GitHub: {response.text}", status_code=response.status_code)

    except Exception as e:
        logging.error(f"Error in processing: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)
