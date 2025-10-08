# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Install required packages (run this in a cell first)
%pip install azure-keyvault-secrets azure-identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# Import necessary libraries
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import os

# Your Key Vault URL
key_vault_url = "https://minimba-enova-kv.vault.azure.net/"

# Create credential object
credential = DefaultAzureCredential()

# Create Key Vault client
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve a secret (replace 'your-secret-name' with actual secret name)
try:
    secret = client.get_secret("minimba-enova-kv")
    print(f"Secret value: {secret.value}")
except Exception as e:
    print(f"Error retrieving secret: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install azure-keyvault-secrets azure-identity

from azure.keyvault.secrets import SecretClient
from azure.identity import InteractiveBrowserCredential

# Your Key Vault URL
key_vault_url = "https://minimba-enova-kv.vault.azure.net/"

# Use interactive browser authentication
credential = InteractiveBrowserCredential()

# Create Key Vault client
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve a secret
try:
    secret = client.get_secret("minimba-enova-kv")
    print(f"Secret retrieved successfully")
    # Don't print the actual secret value for security
except Exception as e:
    print(f"Error retrieving secret: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.keyvault.secrets import SecretClient
from azure.identity import InteractiveBrowserCredential

# Your Key Vault URL
key_vault_url = "https://minimba-enova-kv.vault.azure.net/"

# Specify the tenant ID explicitly
tenant_id = "20b6289e-2339-4be4-b931-402e9c5c8789"

# Use interactive browser authentication with specific tenant
credential = InteractiveBrowserCredential(
    tenant_id=tenant_id,
    additionally_allowed_tenants=["*"]  # Allow any tenant
)

# Create Key Vault client
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve a secret
try:
    secret = client.get_secret("minimba-enova-kv")
    print("Secret retrieved successfully")
    # Don't print the actual secret value for security
except Exception as e:
    print(f"Error retrieving secret: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.keyvault.secrets import SecretClient
from azure.identity import DeviceCodeCredential

key_vault_url = "https://minimba-enova-kv.vault.azure.net/"
tenant_id = "20b6289e-2339-4be4-b931-402e9c5c8789"

# Device code authentication with specific tenant
credential = DeviceCodeCredential(
    tenant_id=tenant_id,
    additionally_allowed_tenants=["*"]
)

client = SecretClient(vault_url=key_vault_url, credential=credential)

try:
    secret = client.get_secret("minimba-enova-kv")
    print("Secret retrieved successfully")
except Exception as e:
    print(f"Error retrieving secret: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import InteractiveBrowserCredential
import requests

# Get token and check tenant info
credential = InteractiveBrowserCredential()
token = credential.get_token("https://management.azure.com/.default")

# Decode the token to see tenant info (optional)
import base64
import json

# Split the token and decode the payload
token_parts = token.token.split('.')
payload = token_parts[1]

# Add padding if needed
payload += '=' * (4 - len(payload) % 4)

decoded = base64.b64decode(payload)
token_info = json.loads(decoded)

print(f"Token tenant ID: {token_info.get('tid', 'Not found')}")
print(f"Issuer: {token_info.get('iss', 'Not found')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.keyvault.secrets import SecretClient
from azure.identity import InteractiveBrowserCredential

key_vault_url = "https://minimba-enova-kv.vault.azure.net/"

# Try with allowing all tenants
credential = InteractiveBrowserCredential(
    additionally_allowed_tenants=["*"]
)

# Or try with the specific tenant from the error
credential = InteractiveBrowserCredential(
    tenant_id="20b6289e-2339-4be4-b931-402e9c5c8789"
)

client = SecretClient(vault_url=key_vault_url, credential=credential)

try:
    secret = client.get_secret("minimba-enova-kv")
    print("Secret retrieved successfully")
except Exception as e:
    print(f"Error retrieving secret: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import InteractiveBrowserCredential
import requests

try:
    # Try to get a token for Key Vault
    credential = InteractiveBrowserCredential()
    
    # Try different scopes
    scopes_to_try = [
        "https://vault.azure.net/.default",
        "https://management.azure.com/.default"
    ]
    
    for scope in scopes_to_try:
        try:
            token = credential.get_token(scope)
            print(f"Successfully got token for scope: {scope}")
            break
        except Exception as e:
            print(f"Failed to get token for {scope}: {e}")
            
except Exception as e:
    print(f"Authentication failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.keyvault.secrets import SecretClient
from azure.identity import DeviceCodeCredential

key_vault_url = "https://minimba-enova-kv.vault.azure.net/"
tenant_id = "20b6289e-2339-4be4-b931-402e9c5c8789"

# Device code flow - will give you a code to enter in browser
def device_code_callback(verification_uri, user_code, expires_in):
    print(f"To sign in, use a web browser to open the page {verification_uri}")
    print(f"and enter the code {user_code} to authenticate.")

credential = DeviceCodeCredential(
    tenant_id=tenant_id,
    prompt_callback=device_code_callback,
    additionally_allowed_tenants=["*"]
)

client = SecretClient(vault_url=key_vault_url, credential=credential)

try:
    secret = client.get_secret("minimba-enova-kv")
    print("Secret retrieved successfully")
except Exception as e:
    print(f"Error retrieving secret: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

# Replace these with your service principal details
tenant_id = "20b6289e-2339-4be4-b931-402e9c5c8789"
client_id = "your-app-registration-client-id"
client_secret = "minimba-enova-kv"
key_vault_url = "https://minimba-enova-kv.vault.azure.net/"

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

client = SecretClient(vault_url=key_vault_url, credential=credential)

try:
    secret = client.get_secret("minimba-enova-kv")
    print("Secret retrieved successfully")
    # Use secret.value to get the actual value
except Exception as e:
    print(f"Error retrieving secret: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
