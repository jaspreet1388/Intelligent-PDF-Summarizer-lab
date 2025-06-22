import logging
from azure.storage.blob import BlobServiceClient

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFeqCnr+HGZfYkQCCdq9l8t+8Jyo1Z4Azm1i6Hg==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

try:
    blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONN_STR)
    containers = blob_service_client.list_containers()
    container_names = [c['name'] for c in containers]
    print("✅ Containers found:", container_names)
except Exception as e:
    print("⚠️ Error:", e)

