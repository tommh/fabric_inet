# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Microsoft Fabric Notebook - Complete Enova Data Processing System
# Adapted from main.py with secure API key management

# Cell 1: Imports and Setup
import requests
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Dict, Any, List
import getpass
from pathlib import Path

# Try to import Fabric utilities for Key Vault access
try:
    from notebookutils import mssparkutils
    KEYVAULT_AVAILABLE = True
    print("✓ Fabric Key Vault integration available")
except ImportError:
    KEYVAULT_AVAILABLE = False
    print("⚠️ Key Vault integration not available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric Notebook - Complete Enova Data Processing System
# Adapted from main.py with secure API key management

# Cell 1: Imports and Setup
import requests
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Dict, Any, List
import getpass
from pathlib import Path

# Try to import Fabric utilities for Key Vault access
try:
    from notebookutils import mssparkutils
    KEYVAULT_AVAILABLE = True
    print("✓ Fabric Key Vault integration available")
except ImportError:
    KEYVAULT_AVAILABLE = False
    print("⚠️ Key Vault integration not available")

# Cell 2: Secure Configuration Class
class FabricEnovaConfig:
    """
    Secure configuration for Fabric environment
    Tries multiple methods to get the API key in order of security preference
    """
    
    def __init__(self, keyvault_name=None, secret_name=None, use_interactive=True):
        # Base configuration
        self.ENOVA_API_BASE_URL = "https://api.enova.no/energisertifikat/v1"
        self.OUTPUT_DIR = "/lakehouse/default/Files/raw_data"
        self.ENOVA_API_KEY = None
        
        # Load API key using secure methods
        self._load_api_key(keyvault_name, secret_name, use_interactive)
        
        if not self.ENOVA_API_KEY:
            raise ValueError("❌ Could not load API key from any source")
    
    def _load_api_key(self, keyvault_name, secret_name, use_interactive):
        """Try multiple methods to load the API key"""
        
        # Method 1: Try Key Vault (most secure)
        if KEYVAULT_AVAILABLE and keyvault_name and secret_name:
            try:
                self.ENOVA_API_KEY = mssparkutils.credentials.getSecret(
                    keyvault_name, 
                    secret_name
                )
                print("✓ API key loaded from Azure Key Vault")
                return
            except Exception as e:
                print(f"⚠️ Key Vault failed: {e}")
        
        # Method 2: Try environment variable
        env_key = os.getenv('ENOVA_API_KEY')
        if env_key:
            self.ENOVA_API_KEY = env_key
            print("✓ API key loaded from environment variable")
            return
        
        # Method 3: Interactive input (development only)
        if use_interactive:
            print("⚠️ No API key found in secure storage.")
            try:
                self.ENOVA_API_KEY = getpass.getpass("Enter your Enova API key: ")
                if self.ENOVA_API_KEY:
                    print("✓ API key entered interactively")
                    return
            except Exception as e:
                print(f"❌ Interactive input failed: {e}")
        
        print("❌ No API key could be loaded from any source")
    
    def get_config_summary(self):
        """Get configuration summary for display"""
        return {
            "API Base URL": self.ENOVA_API_BASE_URL,
            "Output Directory": self.OUTPUT_DIR,
            "API Key Status": "✓ Set" if self.ENOVA_API_KEY else "❌ Not set",
            "Key Vault Available": "✓ Yes" if KEYVAULT_AVAILABLE else "❌ No"
        }

# Cell 3: File Downloader Class
class FabricFileDownloader:
    """Service for downloading Certificate data files from Enova API in Fabric"""
    
    def __init__(self, config):
        self.config = config
        
        if not config.ENOVA_API_KEY:
            raise ValueError("No API key available in configuration")
        
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
        
        # Headers
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "x-api-key": self.config.ENOVA_API_KEY
        }
        
        session.headers.update(headers)
        return session
    
    def download_year_data(self, year: int, output_dir: str = None, force_download: bool = False) -> Dict[str, Any]:
        """Download CSV data for a specific year from Enova API"""
        try:
            # Rate limiting
            if self.api_call_count > 0:
                time.sleep(0.5)
            
            # Step 1: Get the file URL from the API
            print(f"📡 Requesting file information for year {year}...")
            api_url = f"{self.base_url}/{year}"
            
            response = self.session.get(api_url, timeout=30)
            self.api_call_count += 1
            
            # Handle rate limiting
            if response.status_code == 429:
                print(f"⏱️ Rate limited on year {year}, waiting 60 seconds...")
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
            
            print(f"📅 Found data file for period: {from_date} to {to_date}")
            
            # Step 2: Determine output file path
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
                print(f"📂 File already exists: {file_path} ({file_size:,} bytes) - skipping download")
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
            print(f"⬇️ Downloading CSV file from: {bank_file_url}")
            csv_response = self.session.get(bank_file_url, timeout=30)
            csv_response.raise_for_status()
            
            # Write file
            with open(file_path, 'wb') as f:
                f.write(csv_response.content)
            
            final_size = os.path.getsize(file_path)
            self.download_count += 1
            print(f"✅ Successfully downloaded: {file_path} ({final_size:,} bytes)")
            
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

# Cell 4: Main Processing Functions (adapted from main.py)
class EnovaProcessor:
    """Main processor class with all the functionality from main.py"""
    
    def __init__(self, config):
        self.config = config
        self.downloader = FabricFileDownloader(config)
    
    def download_year_data(self, year: int, force: bool = False) -> bool:
        """Download CSV data for a specific year"""
        print(f"📥 Downloading data for year {year}...")
        
        result = self.downloader.download_year_data(year=year, force_download=force)
        
        if result['success']:
            print(f"✅ Success: {result['file_path']}")
            print(f"   📅 Date range: {result['from_date']} to {result['to_date']}")
            print(f"   📊 File size: {result['file_size']:,} bytes")
            if not result.get('downloaded', False):
                print("   ℹ️ (File already existed)")
            return True
        else:
            print(f"❌ Failed: {result['error']}")
            return False
    
    def download_multiple_years(self, start_year: int, end_year: int, force: bool = False) -> bool:
        """Download CSV data for multiple years"""
        print(f"📥 Downloading data for years {start_year} to {end_year}")
        print("=" * 60)
        
        success_count = 0
        total_count = end_year - start_year + 1
        
        for year in range(start_year, end_year + 1):
            print(f"\n🗓️ Processing year {year} ({year - start_year + 1}/{total_count})")
            if self.download_year_data(year, force):
                success_count += 1
        
        print(f"\n📊 Summary: Downloaded {success_count}/{total_count} years successfully")
        if success_count == total_count:
            print("🎉 All downloads completed successfully!")
        else:
            print(f"⚠️ {total_count - success_count} downloads failed")
        
        return success_count == total_count
    
    def list_downloaded_files(self) -> List[str]:
        """List all downloaded CSV files"""
        output_dir = self.config.OUTPUT_DIR
        
        if not os.path.exists(output_dir):
            print(f"📁 Download directory does not exist: {output_dir}")
            return []
        
        # Find CSV files
        csv_files = []
        for file in os.listdir(output_dir):
            if file.startswith('enova_data_') and file.endswith('.csv'):
                csv_files.append(file)
        
        if not csv_files:
            print("📂 No CSV files found")
            return []
        
        print(f"📋 Found {len(csv_files)} CSV files:")
        total_size = 0
        
        for filename in sorted(csv_files):
            file_path = os.path.join(output_dir, filename)
            size = os.path.getsize(file_path)
            total_size += size
            modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            year = filename.replace('enova_data_', '').replace('.csv', '')
            print(f"   📄 {year}: {filename} ({size:,} bytes) - {modified.strftime('%Y-%m-%d %H:%M')}")
        
        print(f"\n📊 Total: {len(csv_files)} files, {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
        return csv_files
    
    def cleanup_old_files(self, days_old: int = 30) -> int:
        """Remove files older than specified days"""
        output_dir = self.config.OUTPUT_DIR
        
        if not os.path.exists(output_dir):
            print(f"📁 Directory {output_dir} does not exist")
            return 0
        
        import time as time_module
        cutoff_time = time_module.time() - (days_old * 24 * 60 * 60)
        
        csv_files = [f for f in os.listdir(output_dir) if f.startswith('enova_data_') and f.endswith('.csv')]
        removed_count = 0
        
        print(f"🧹 Cleaning up files older than {days_old} days...")
        
        for filename in csv_files:
            file_path = os.path.join(output_dir, filename)
            if os.path.getmtime(file_path) < cutoff_time:
                os.remove(file_path)
                print(f"   🗑️ Removed old file: {filename}")
                removed_count += 1
        
        if removed_count > 0:
            print(f"✅ Removed {removed_count} old files")
        else:
            print("ℹ️ No old files found to remove")
        
        return removed_count
    
    def show_config(self):
        """Show current configuration"""
        print("⚙️ Current Configuration:")
        print("=" * 50)
        summary = self.config.get_config_summary()
        for key, value in summary.items():
            print(f"   {key}: {value}")
        print("=" * 50)

# Cell 5: Configuration Setup
def setup_configuration():
    """Setup configuration with examples"""
    print("""
🔐 CONFIGURATION SETUP OPTIONS:

1️⃣ Key Vault (Recommended for Production):
   config = FabricEnovaConfig(
       keyvault_name='your-keyvault-name',
       secret_name='enova-api-key'
   )

2️⃣ Environment Variable:
   os.environ['ENOVA_API_KEY'] = 'your-api-key-here'
   config = FabricEnovaConfig()

3️⃣ Interactive Input (Development):
   config = FabricEnovaConfig()  # Will prompt for API key

Choose one approach based on your needs.
""")

setup_configuration()

# Cell 6: Initialize Configuration
# Choose your configuration method:

# Option 1: For production with Key Vault (uncomment and modify):
# config = FabricEnovaConfig(
#     keyvault_name='your-keyvault-name',  # Replace with your Key Vault name
#     secret_name='enova-api-key'          # Replace with your secret name
# )

# Option 2: For development/testing (will prompt for API key):
config = FabricEnovaConfig()

# Option 3: Using environment variable (uncomment to use):
# os.environ['ENOVA_API_KEY'] = 'your-key-here'  # Set this first
# config = FabricEnovaConfig(use_interactive=False)

print("🎯 Configuration loaded successfully!")

# Cell 7: Initialize Processor and Show Status
processor = EnovaProcessor(config)
processor.show_config()

# Cell 8: Quick Test - Download Single Year
# Test download for a single year
test_year = 2023  # Change this to the year you want to test

print(f"\n🧪 Testing download for year {test_year}")
print("=" * 40)
success = processor.download_year_data(test_year)

if success:
    print(f"\n✅ Test successful! Ready for bulk operations.")
else:
    print(f"\n❌ Test failed. Please check your API key and configuration.")

# Cell 9: Bulk Download Operations
def bulk_download_example():
    """Example function for bulk downloading"""
    print("""
📦 BULK DOWNLOAD EXAMPLES:

# Download multiple years:
processor.download_multiple_years(2020, 2024)

# Download with force (re-download existing files):
processor.download_multiple_years(2020, 2024, force=True)

# Download single year with force:
processor.download_year_data(2023, force=True)
""")

bulk_download_example()

# Uncomment the following lines to perform bulk downloads:

# Download multiple years (2020-2024)
# processor.download_multiple_years(2020, 2024)

# Download just recent years
# processor.download_multiple_years(2022, 2024)

# Cell 10: File Management Operations
print("\n📁 FILE MANAGEMENT OPERATIONS:")
print("=" * 40)

# List current files
current_files = processor.list_downloaded_files()

# Show cleanup options
print(f"""
🧹 CLEANUP OPTIONS:
   
# Remove files older than 30 days:
# processor.cleanup_old_files(30)

# Remove files older than 7 days:
# processor.cleanup_old_files(7)
""")

# Cell 11: Utility Functions
def download_years_interactive():
    """Interactive function to download specific years"""
    try:
        years_input = input("Enter years to download (comma-separated, e.g., 2020,2021,2022): ")
        force_input = input("Force re-download existing files? (y/n): ").lower().strip()
        
        years = [int(year.strip()) for year in years_input.split(',')]
        force = force_input == 'y'
        
        print(f"\n📥 Starting download for years: {years}")
        print(f"🔄 Force re-download: {force}")
        
        success_count = 0
        for year in years:
            if processor.download_year_data(year, force):
                success_count += 1
        
        print(f"\n📊 Downloaded {success_count}/{len(years)} years successfully")
        
    except Exception as e:
        print(f"❌ Error in interactive download: {e}")

def download_year_range_interactive():
    """Interactive function to download a range of years"""
    try:
        start_year = int(input("Enter start year: "))
        end_year = int(input("Enter end year: "))
        force_input = input("Force re-download existing files? (y/n): ").lower().strip()
        
        force = force_input == 'y'
        
        processor.download_multiple_years(start_year, end_year, force)
        
    except Exception as e:
        print(f"❌ Error in range download: {e}")

print("""
🎮 INTERACTIVE FUNCTIONS AVAILABLE:

# Download specific years interactively:
# download_years_interactive()

# Download year range interactively:
# download_year_range_interactive()

# Show current files:
# processor.list_downloaded_files()

# Show configuration:
# processor.show_config()
""")

# Cell 12: Data Inspection (if you have pandas available)
def inspect_downloaded_data(year=None):
    """Inspect downloaded CSV data"""
    try:
        import pandas as pd
        
        output_dir = config.OUTPUT_DIR
        
        if year:
            filename = f"enova_data_{year}.csv"
            file_path = os.path.join(output_dir, filename)
            
            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
                return
            
            print(f"📊 Inspecting data for year {year}")
            df = pd.read_csv(file_path)
            
        else:
            # Find most recent file
            csv_files = [f for f in os.listdir(output_dir) if f.startswith('enova_data_') and f.endswith('.csv')]
            if not csv_files:
                print("❌ No CSV files found")
                return
            
            latest_file = sorted(csv_files)[-1]
            file_path = os.path.join(output_dir, latest_file)
            year = latest_file.replace('enova_data_', '').replace('.csv', '')
            
            print(f"📊 Inspecting latest data file: {latest_file}")
            df = pd.read_csv(file_path)
        
        print(f"📏 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        print(f"📋 Columns: {list(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}")
        print(f"💾 Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        
        print("\n📝 First few rows:")
        print(df.head())
        
        return df
        
    except ImportError:
        print("❌ pandas not available. Install with: !pip install pandas")
    except Exception as e:
        print(f"❌ Error inspecting data: {e}")

print("""
🔍 DATA INSPECTION:

# Inspect specific year:
# df = inspect_downloaded_data(2023)

# Inspect latest downloaded file:
# df = inspect_downloaded_data()
""")

print("\n🎉 Setup complete! You can now use the processor to download Enova data.")
print("📖 See the examples above for common operations.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
