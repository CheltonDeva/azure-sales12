import logging
import azure.functions as func
import csv
import io
from azure.storage.blob import BlobServiceClient

# Storage account details (you can also store these in Azure App Settings)
STORAGE_ACCOUNT_NAME = "dailysalesstorage947"
STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=..."  # from Access keys in Storage

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('FusionPractices12 function triggered.')

    try:
        # Parse JSON body from ADF
        body = req.get_json()
        container = body.get("container")
        file_path = body.get("filePath")

        if not container or not file_path:
            return func.HttpResponse("Invalid request: Missing container or filePath", status_code=400)

        # Connect to Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=container, blob=file_path)

        # Download blob content
        blob_data = blob_client.download_blob().readall().decode("utf-8")
        reader = csv.DictReader(io.StringIO(blob_data))

        required_fields = ["TransactionID", "ProductName", "Amount"]

        for row in reader:
            # Validate required fields
            for field in required_fields:
                if not row.get(field):
                    return func.HttpResponse("Invalid Data: Missing field", status_code=400)

            # Validate Amount >= 0
            if float(row["Amount"]) < 0:
                return func.HttpResponse("Invalid Data: Negative Amount", status_code=400)

        return func.HttpResponse("Validation Passed", status_code=200)

    except Exception as e:
        logging.error(f"Validation error: {str(e)}")
        return func.HttpResponse(f"Invalid Data: {str(e)}", status_code=500)

