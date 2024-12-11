import re
import signal
import sys
from typing import Optional, List, Dict, Any
import requests
import json
import csv
from pathlib import Path
import logging
import urllib.parse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from multiprocessing import cpu_count

# Configuration for data files
REGION_FILES = {
    'provinsi': 'provinsi.csv',                # Province level data
    'kabkota': 'kabupaten_kota.csv',          # City/Regency level data
    'kecamatan': 'kecamatan.csv',             # District level data
    'kelurahan': 'kelurahan.csv'              # Village/Sub-district level data
}

# API and processing settings
KPU_JSON_URL = 'https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/'
DELAY_BETWEEN_REQUESTS = 2  # seconds
PARALLEL_WORKERS = min(max(1, cpu_count() - 1), 36)  # Cap at 36 parallel workers (number of concurrent downloads)

# Set up logging to track progress and errors
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

class RegionalDataFetcher:
    """
    Downloads data hierarchically: Provinsi -> Kabkot -> Kecamatan -> Kelurahan
    """
    def __init__(self):
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,   # Increase total number of connections
            pool_maxsize=100,       # Increase maximum number of connections
            max_retries=3,          # Add retry capability
            pool_block=False        # Don't block when pool is full
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.is_active = True

        self.is_active = True
        # Handle graceful shutdown on Ctrl+C
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Ensures clean program termination when user presses Ctrl+C"""
        logger.info("\nShutting down gracefully... Please wait.")
        self.is_active = False
        self.session.close()
        sys.exit(0)

    @lru_cache(maxsize=128)
    def is_file_empty(self, filepath: str) -> bool:
        """Check if a file exists and has content"""
        try:
            return Path(filepath).stat().st_size == 0
        except FileNotFoundError:
            return True

    def download_data(self, url: str) -> Optional[bytes]:
        """Download data from KPU JSON with connection management"""
        if not self.is_active:
            return None
        try:
            logger.info(f'Downloading from: {url}')
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
            # Add retry logic with exponential backoff
            for attempt in range(3):
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    return response.content
                except requests.RequestException as e:
                    if attempt == 2:  # Last attempt
                        raise
                    wait_time = (2 ** attempt) * DELAY_BETWEEN_REQUESTS
                    logger.warning(f'Retry {attempt + 1}/3 after {wait_time}s: {e}')
                    time.sleep(wait_time)
                    
        except requests.RequestException as e:
            logger.error(f'Download failed: {e}')
            return None

    def save_to_csv(self, data: bytes, output_file: str) -> None:
        """Save downloaded data to CSV file with proper formatting"""
        if not self.is_active:
            return
        try:
            records = json.loads(data)
            if not records:
                return

            file_exists = Path(output_file).exists()
            write_mode = 'a' if file_exists else 'w'
            
            with open(output_file, write_mode, newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=records[0].keys())
                if not file_exists:
                    writer.writeheader()
                
                # Write records, excluding overseas regions and formatting names
                writer.writerows(
                    {**record, 'nama': self._format_region_name(record['nama'])}
                    for record in records 
                    if record['nama'] != 'Luar Negeri'
                )
        except (json.JSONDecodeError, csv.Error) as e:
            logger.error(f'Error saving data: {e}')

    @staticmethod
    def _format_region_name(name: str) -> str:
        """Format region names according"""
        if name == 'P A P U A':  # Special case for Papua
            return 'Papua'
        return ' '.join(
            # Keep Roman numerals as-is, capitalize other words
            word if re.match(r'^[LXIVCDM]+$', word) else word.capitalize()
            for word in name.strip().split()
        )

    def load_csv_data(self, filename: str) -> List[Dict[str, Any]]:
        """Read data from CSV file"""
        if not self.is_active:
            return []
        try:
            with open(filename, newline='', encoding='utf-8') as f:
                return list(csv.DictReader(f))
        except FileNotFoundError:
            logger.error(f'Missing file: {filename}')
            return []

    def process_region(self, region_code: str, output_file: str) -> None:
        """Process a single region's data"""
        if not self.is_active:
            return

        # Build the appropriate URL path based on region type
        if output_file == REGION_FILES['kecamatan']:
            path_uri = f"{region_code[:2]}/{region_code}.json"
        elif output_file == REGION_FILES['kelurahan']:
            path_uri = f"{region_code[:2]}/{region_code[:4]}/{region_code}.json"
        else:
            path_uri = f"{region_code}.json"

        url = urllib.parse.urljoin(KPU_JSON_URL, path_uri)
        if downloaded_data := self.download_data(url):
            self.save_to_csv(downloaded_data, output_file)

    def process_region_level(self, regions: List[Dict[str, Any]], output_file: str, executor: ThreadPoolExecutor) -> None:
        """Process all regions at a particular administrative level in parallel"""
        if not regions or not self.is_active:
            return
        
        # Create download tasks for all regions
        download_tasks = [
            executor.submit(self.process_region, region['kode'], output_file)
            for region in regions
        ]
        
        # Wait for all downloads to complete
        for task in as_completed(download_tasks):
            if not self.is_active:
                break
            try:
                task.result()
            except Exception as e:
                logger.error(f"Failed to process region: {e}")

    def download_all_regions(self):
        """Download all regional data in hierarchical order"""
        try:
            # Start with provinces (level 1)
            if self.is_file_empty(REGION_FILES['provinsi']):
                self.process_region('0', REGION_FILES['provinsi'])

            # Process remaining levels in parallel
            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
                # Level 2: Cities/Regencies
                if self.is_active:
                    provinces = self.load_csv_data(REGION_FILES['provinsi'])
                    self.process_region_level(provinces, REGION_FILES['kabkota'], executor)

                # Level 3: Districts
                if self.is_active:
                    cities = self.load_csv_data(REGION_FILES['kabkota'])
                    self.process_region_level(cities, REGION_FILES['kecamatan'], executor)

                # Level 4: Villages/Sub-districts
                if self.is_active:
                    districts = self.load_csv_data(REGION_FILES['kecamatan'])
                    self.process_region_level(districts, REGION_FILES['kelurahan'], executor)

        except Exception as e:
            logger.error(f"Download process failed: {e}")
            self.is_active = False
            raise
        finally:
            self.session.close()

def main():
    """Main entry point of the program"""
    try:
        downloader = RegionalDataFetcher()
        downloader.download_all_regions()
        if downloader.is_active:
            logger.info("Successfully downloaded all regional data!")
    except KeyboardInterrupt:
        logger.info("\nDownload cancelled by user")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        sys.exit(0)

if __name__ == "__main__":
    main()