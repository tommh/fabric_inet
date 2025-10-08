# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eaac67af-c405-4e6e-9ccd-6379eac68996",
# META       "default_lakehouse_name": "EnergyCertificates",
# META       "default_lakehouse_workspace_id": "160eaa2f-7866-4438-9ffa-3630c9e77da1",
# META       "known_lakehouses": [
# META         {
# META           "id": "eaac67af-c405-4e6e-9ccd-6379eac68996"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: Install required packages and imports
import requests
import time
import os
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Dict, Any

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Configuration
class Config:
    def __init__(self):
        # Set your API key here (replace with your actual key)
        self.ENOVA_API_KEY = "4bc33f50357049c3afa8b59858cd70f2"  # API key
        self.ENOVA_API_BASE_URL = "https://api.enova.no/energisertifikat/v1"  # Adjust if needed
        
        # OneLake path for Fabric
        self.OUTPUT_DIR = "/lakehouse/default/Files/raw_data"

config = Config()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: FileDownloader class adapted for Fabric
class FileDownloader:
    """Service for downloading Certificate data files from Enova API"""
    
    def __init__(self, config):
        self.config = config
        self.base_url = config.ENOVA_API_BASE_URL + "/Fil"
        self.session = self._setup_session()
        self.download_count = 0
        self.api_call_count = 0
    
    def _setup_session(self):
        """Setup requests session with retry strategy and headers"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Use the exact same headers that worked in your original code
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        
        # Add API key using x-api-key header (as in your working code)
        if self.config.ENOVA_API_KEY and self.config.ENOVA_API_KEY != "YOUR_API_KEY_HERE":
            headers["x-api-key"] = self.config.ENOVA_API_KEY
        
        session.headers.update(headers)
        return session
    
    def download_year_data(self, year: int, output_dir: str = None, force_download: bool = False) -> Dict[str, Any]:
        """
        Download CSV data for a specific year from Enova API
        """
        try:
            # Rate limiting
            if self.api_call_count > 0:
                time.sleep(0.5)  # Default delay
            
            # Step 1: Get the file URL from the API
            print(f"Requesting file information for year {year}...")
            api_url = f"{self.base_url}/{year}"
            
            response = self.session.get(api_url, timeout=30)
            self.api_call_count += 1
            
            # Handle rate limiting
            if response.status_code == 429:
                print(f"Rate limited on year {year}, waiting 60 seconds...")
                time.sleep(60)
                response = self.session.get(api_url, timeout=30)
                self.api_call_count += 1
            
            if response.status_code == 404:
                return {
                    'success': False,
                    'error': f'No data available for year {year}',
                    'status_code': 404
                }
            
            response.raise_for_status()
            data = response.json()
            
            # Extract information from API response
            from_date = data.get('fromDate')
            to_date = data.get('toDate')
            bank_file_url = data.get('bankFileUrl')
            
            if not bank_file_url:
                return {
                    'success': False,
                    'error': 'No bankFileUrl found in API response'
                }
            
            print(f"Found data file for period: {from_date} to {to_date}")
            
            # Step 2: Determine output file path (adapted for Fabric)
            if not output_dir:
                output_dir = self.config.OUTPUT_DIR
            
            # Create directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Create filename
            filename = f"enova_data_{year}.csv"
            file_path = os.path.join(output_dir, filename)
            
            # Step 3: Check if file already exists
            if os.path.exists(file_path) and not force_download:
                file_size = os.path.getsize(file_path)
                print(f"File already exists: {file_path} ({file_size:,} bytes) - skipping download")
                return {
                    'success': True,
                    'file_path': file_path,
                    'from_date': from_date,
                    'to_date': to_date,
                    'file_size': file_size,
                    'downloaded': False,
                    'message': 'File already exists'
                }
            
            # Step 4: Download the CSV file
            print(f"Downloading CSV file from: {bank_file_url}")
            csv_response = self.session.get(bank_file_url, timeout=30)
            csv_response.raise_for_status()
            
            # Write file
            with open(file_path, 'wb') as f:
                f.write(csv_response.content)
            
            final_size = os.path.getsize(file_path)
            self.download_count += 1
            print(f"Successfully downloaded: {file_path} ({final_size:,} bytes)")
            
            return {
                'success': True,
                'file_path': file_path,
                'from_date': from_date,
                'to_date': to_date,
                'file_size': final_size,
                'downloaded': True
            }
            
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': f'HTTP request failed: {str(e)}'
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}'
            }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Test with a single year
# Replace YOUR_API_KEY_HERE in the Config class above with your actual API key first!

# Test download for a single year
downloader = FileDownloader(config)
test_year = 2025  # Change to whatever year you want to test
result = downloader.download_year_data(test_year)

print("\nDownload result:")
for key, value in result.items():
    print(f"{key}: {value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric Notebook - Enova Data Downloader
# Adapted for OneLake storage

# Cell 1: Install required packages and imports
import requests
import time
import os
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Dict, Any

# Cell 2: Configuration
class Config:
    def __init__(self):
        # Set your API key here (replace with your actual key)
        self.ENOVA_API_KEY = "4bc33f50357049c3afa8b59858cd70f2"  # API key
        self.ENOVA_API_BASE_URL = "https://api.enova.no/energisertifikat/v1"  # Adjust if needed
        
        # OneLake path for Fabric
        self.OUTPUT_DIR = "/lakehouse/default/Files/raw_data"

config = Config()

# Cell 3: FileDownloader class adapted for Fabric
class FileDownloader:
    """Service for downloading Certificate data files from Enova API"""
    
    def __init__(self, config):
        self.config = config
        self.base_url = config.ENOVA_API_BASE_URL + "/Fil"
        self.session = self._setup_session()
        self.download_count = 0
        self.api_call_count = 0
    
    def _setup_session(self):
        """Setup requests session with retry strategy and headers"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Use the exact same headers that worked in your original code
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        
        # Add API key using x-api-key header (as in your working code)
        if self.config.ENOVA_API_KEY and self.config.ENOVA_API_KEY != "YOUR_API_KEY_HERE":
            headers["x-api-key"] = self.config.ENOVA_API_KEY
        
        session.headers.update(headers)
        return session
    
    def download_year_data(self, year: int, output_dir: str = None, force_download: bool = False) -> Dict[str, Any]:
        """
        Download CSV data for a specific year from Enova API
        """
        try:
            # Rate limiting
            if self.api_call_count > 0:
                time.sleep(0.5)  # Default delay
            
            # Step 1: Get the file URL from the API
            print(f"Requesting file information for year {year}...")
            api_url = f"{self.base_url}/{year}"
            
            response = self.session.get(api_url, timeout=30)
            self.api_call_count += 1
            
            # Handle rate limiting
            if response.status_code == 429:
                print(f"Rate limited on year {year}, waiting 60 seconds...")
                time.sleep(60)
                response = self.session.get(api_url, timeout=30)
                self.api_call_count += 1
            
            if response.status_code == 404:
                return {
                    'success': False,
                    'error': f'No data available for year {year}',
                    'status_code': 404
                }
            
            response.raise_for_status()
            data = response.json()
            
            # Extract information from API response
            from_date = data.get('fromDate')
            to_date = data.get('toDate')
            bank_file_url = data.get('bankFileUrl')
            
            if not bank_file_url:
                return {
                    'success': False,
                    'error': 'No bankFileUrl found in API response'
                }
            
            print(f"Found data file for period: {from_date} to {to_date}")
            
            # Step 2: Determine output file path (adapted for Fabric)
            if not output_dir:
                output_dir = self.config.OUTPUT_DIR
            
            # Create directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Create filename
            filename = f"enova_data_{year}.csv"
            file_path = os.path.join(output_dir, filename)
            
            # Step 3: Check if file already exists
            if os.path.exists(file_path) and not force_download:
                file_size = os.path.getsize(file_path)
                print(f"File already exists: {file_path} ({file_size:,} bytes) - skipping download")
                return {
                    'success': True,
                    'file_path': file_path,
                    'from_date': from_date,
                    'to_date': to_date,
                    'file_size': file_size,
                    'downloaded': False,
                    'message': 'File already exists'
                }
            
            # Step 4: Download the CSV file
            print(f"Downloading CSV file from: {bank_file_url}")
            csv_response = self.session.get(bank_file_url, timeout=30)
            csv_response.raise_for_status()
            
            # Write file
            with open(file_path, 'wb') as f:
                f.write(csv_response.content)
            
            final_size = os.path.getsize(file_path)
            self.download_count += 1
            print(f"Successfully downloaded: {file_path} ({final_size:,} bytes)")
            
            return {
                'success': True,
                'file_path': file_path,
                'from_date': from_date,
                'to_date': to_date,
                'file_size': final_size,
                'downloaded': True
            }
            
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': f'HTTP request failed: {str(e)}'
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}'
            }

# Cell 4: Usage example
def download_multiple_years(years: list, force_download: bool = False):
    """Download data for multiple years"""
    downloader = FileDownloader(config)
    results = []
    
    for year in years:
        print(f"\n--- Processing year {year} ---")
        result = downloader.download_year_data(year, force_download=force_download)
        results.append({'year': year, **result})
        
        if not result['success']:
            print(f"Failed to download {year}: {result.get('error', 'Unknown error')}")
        else:
            print(f"Year {year}: {'Downloaded' if result['downloaded'] else 'Already exists'}")
    
    return results

# Cell 5: Test with a single year
# Replace YOUR_API_KEY_HERE in the Config class above with your actual API key first!

# Test download for a single year
downloader = FileDownloader(config)
test_year = 2023  # Change to whatever year you want to test
result = downloader.download_year_data(test_year)

print("\nDownload result:")
for key, value in result.items():
    print(f"{key}: {value}")

# Cell 6: Download multiple years (optional)
# Uncomment and run this cell to download multiple years
"""
years_to_download = [2020, 2021, 2022, 2023, 2024]
results = download_multiple_years(years_to_download)

print(f"\n--- Summary ---")
successful = sum(1 for r in results if r['success'])
print(f"Successfully processed: {successful}/{len(results)} years")
"""

# Cell 7: Check downloaded files
def list_downloaded_files():
    """List all downloaded files in the output directory"""
    output_dir = config.OUTPUT_DIR
    if os.path.exists(output_dir):
        files = [f for f in os.listdir(output_dir) if f.startswith('enova_data_') and f.endswith('.csv')]
        print(f"Downloaded files in {output_dir}:")
        for file in sorted(files):
            file_path = os.path.join(output_dir, file)
            size = os.path.getsize(file_path)
            print(f"  {file} ({size:,} bytes)")
        return files
    else:
        print(f"Output directory {output_dir} does not exist")
        return []

# Run this to see what files you have
list_downloaded_files()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
