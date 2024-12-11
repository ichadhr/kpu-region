import re
from typing import Optional, List, Dict, Any
import requests
import json
import csv
import os
import urllib.parse
import logging
from pathlib import Path
import time

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m-%d-%Y %I:%M:%S %p'
)
logger = logging.getLogger()
global_sleep_time = 2

filename_provinsi       = 'provinsi.csv'
filename_kabupaten_kota = 'kabupaten_kota.csv'
filename_kecamatan      = 'kecamatan.csv'
filename_kelurahan      = 'kelurahan.csv'

# https://pemilu2024.kpu.go.id/pilpres/hitung-suara/
uri_kpu = 'https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/'

def is_file_empty(pathfile: str) -> bool:
    """Check if a file is empty."""
    return not os.path.exists(pathfile) or os.path.getsize(pathfile) == 0

def surf(uri: str) -> Optional[bytes]:
    """
    Fetch data from a given URI.
    
    Args:
        uri: The URI to fetch data from
        
    Returns:
        Response content if successful, None otherwise
    """
    try:
        logger.info('Trying to access: %s', uri)
        time.sleep(global_sleep_time)
        response = requests.get(url=uri, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as err:
        logger.error('Request failed: %s', err)
        return None

def get_data_provinsi(uri: str) -> Optional[str]:
    """
    Get province data and write to CSV.
    
    Args:
        uri: Base URI for the API
        
    Returns:
        Written data if successful, None otherwise
    """
    uri_provinsi = urllib.parse.urljoin(uri, '0.json')
    logger.info('Get data Provinsi')
    data = surf(uri_provinsi)
    if data:
        logger.info('Writing data to CSV')
        return write_csv(data, filename_provinsi)
    return None

def get_data_kabkot(uri: str) -> Optional[bytes]:
    if is_file_empty(filename_provinsi):
        logger.warning('Cannot find data Provinsi?')
        get_data_provinsi(uri)
    list_provinsi = read_csv(filename_provinsi)
    for prov in list_provinsi:
        """Get kabupaten/kota data."""
        provinsi_kode_uri = ''.join((prov['kode'], '.json'))
        logger.info('Get data Kabupaten/Kota from Provinsi: %s', prov['nama'])
        uri_provinsi = urllib.parse.urljoin(uri, provinsi_kode_uri)
        data = surf(uri_provinsi)
        if data:
            logger.info('Writing data to CSV')
            write_csv(data, filename_kabupaten_kota)

def get_data_kecamatan(uri: str) -> Optional[bytes]:
    if is_file_empty(filename_kabupaten_kota):
        logger.warning('Cannot find data Provinsi?')
        get_data_kabkot(uri)
    list_kabkot = read_csv(filename_kabupaten_kota)
    for kabkot in list_kabkot:
        """Get kecamatan data."""
        kabkot_kode_uri = ''.join((kabkot['kode'][:2], '/', kabkot['kode'], '.json'))
        logger.info('Get data Kecamatan from Kabupaten/Kota: %s', kabkot['nama'])
        uri_kabkot = urllib.parse.urljoin(uri, kabkot_kode_uri)
        data = surf(uri_kabkot)
        if data:
            logger.info('Writing data to CSV')
            write_csv(data, filename_kecamatan)

def get_data_kelurahan(uri: str) -> Optional[bytes]:
    if is_file_empty(filename_kecamatan):
        logger.warning('Cannot find data Provinsi?')
        get_data_kecamatan(uri)
    list_kecamatan = read_csv(filename_kecamatan)
    for kecamatan in list_kecamatan:
        """Get kelurahan data."""
        kecamatan_kode_uri = ''.join((kecamatan['kode'][:2], '/', kecamatan['kode'][:4], '/', kecamatan['kode'], '.json'))
        logger.info('Get data Kelurahan from Kecamatan: %s', kecamatan['nama'])
        uri_kecamatan = urllib.parse.urljoin(uri, kecamatan_kode_uri)
        data = surf(uri_kecamatan)
        if data:
            logger.info('Writing data to CSV')
            write_csv(data, filename_kelurahan)

def write_csv(data: bytes, filename: str) -> Optional[str]:
    """
    Write JSON data to a CSV file.
    
    Args:
        data: JSON data in bytes
        filename: Output CSV filename
        
    Returns:
        Path to written file if successful, None otherwise
    """
    try:
        text: List[Dict[str, Any]] = json.loads(data)
        if not text:
            logger.warning('No data to write')
            return None
            
        values = list(text[0].keys())
        with open(filename, 'a', newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, delimiter=',', fieldnames=values, extrasaction='ignore')
            if is_file_empty(filename):
                writer.writeheader()
            for item in text:
                if item['nama'] != 'Luar Negeri': # skip luar negeri
                    item['nama'] = formating_string(item['nama'])
                    writer.writerow(item)
            
        output_path = str(Path(filename).resolve())
        logger.info('Ouput data: %s', output_path)
        return None
        
    except (csv.Error, json.JSONDecodeError) as e:
        logger.error('Error: %s', e)
        return None

def read_csv(filepath: str) -> Optional[List]:
    if is_file_empty(filepath):
        return None
    with open(filepath) as csvfile:
        data_list = []
        data = csv.DictReader(csvfile)
        for row in data:
            row.pop('tingkat')
            # data_dict = {'id': row['B'], 'kode': row['C']}
            data_list.append(row)
    return data_list

def formating_string(nama: str) -> str:
    """
    Capitalizes each word in the input string that is not a Roman numeral.
    
    Returns:
        str: The formatted string with non-Roman numeral words capitalized.
    """
    if nama == 'P A P U A':
        nama = 'Papua'
    pattern = r'\b(?![LXIVCDM]+\b)([A-Z]+)\b'
    result = re.sub('\s+',' ', nama.strip())
    return re.sub(pattern, lambda matche: matche.group(0).capitalize(), result)

get_data_kelurahan(uri_kpu)