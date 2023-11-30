import os
import platform
import pandas as pd
import numpy as np
import time
import asyncio
import threading
import json
import itertools
import nest_asyncio
nest_asyncio.apply()
from bs4 import BeautifulSoup

# for logging
import sys
import logging
import datetime
from logging.handlers import TimedRotatingFileHandler

from urllib.request import urlopen
from urllib.parse import urlparse, parse_qs

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

#logging
class PrintLogger:
    def __init__(self, log):
        self.terminal = sys.stdout
        self.log = log

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass

current_date = datetime.datetime.now().strftime("%Y-%m-%d")
def setup_logging():
    log_formatter = logging.Formatter("%(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    log_file = f'./loggings/verval_scrape_{current_date}.log'
    log_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30, utc=False)
    log_handler.setFormatter(log_formatter)
    log_handler.setLevel(logging.DEBUG)
    
    logger = logging.getLogger()
    logger.addHandler(log_handler)

    sys.stdout = PrintLogger(log_handler.stream)
setup_logging()

os_system = platform.system()
print('OS SYSTEM:   ', os_system)

#cpu count
num_threads = os.cpu_count()
print(f'Num Threads:    {num_threads}')

# set path ke file chromedriver to operate the Chrome browser.
chrome_version = 'v119_0_6045_105'
if os_system == 'Windows':
    chrome_path = os.path.join('webdriver', 'chrome', os_system, chrome_version, 'chromedriver.exe')
elif os_system == 'Linux':
    chrome_path = os.path.join('webdriver', 'chrome', os_system, chrome_version, 'chromedriver')
else:
    chrome_path = os.path.join('webdriver', 'chrome', 'MacOS', chrome_version, 'chromedriver')

print('CHROME PATH:    ', chrome_path)
#webdriver options
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-setuid-sandbox')
#open Browser in maximized mode
chrome_options.add_argument("start-maximized")
#disable extension
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

def driversetup():
  # webdriver_service = ChromeService(ChromeDriverManager().install())
  chrome_service = Service(executable_path=chrome_path)
  driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
  return driver
driver = driversetup()

kode_wilayah_kabupaten = pd.Series([]) 
kode_wilayah_kecamatan = pd.Series([])
sekolah_id_enkrip = pd.Series([])
max_retries = 3

province_api = 'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=0&kode_wilayah=000000&semester_id=20231'
provinces = pd.read_json(province_api)
provinces = provinces.sort_values('kode_wilayah')
provinces = provinces['kode_wilayah']
provinces = provinces.astype(str).str.zfill(6)


"""## get kode wilayah `kabupaten`"""
start_kab_kota = time.time()
# 020000
# 050000
for province in provinces:
    province = province.strip()
    kab_api = 'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=1&kode_wilayah={0}&semester_id=20231'.format(province)

    kabupatens = pd.read_json(kab_api)
    kabupatens = kabupatens.sort_values('kode_wilayah')
    kabupatens = kabupatens['kode_wilayah']
    kode_wilayah_kabupaten = kode_wilayah_kabupaten._append(kabupatens, ignore_index=True)
    # kode_wilayah_kabupaten.append(kabupatens)

kode_wilayah_kabupaten = kode_wilayah_kabupaten.astype(str).str.zfill(6)
df_kode_wilayah_kabupaten = pd.DataFrame({'kode_wilayah_kabupaten': kode_wilayah_kabupaten})
df_kode_wilayah_kabupaten.to_csv('./dataset/trial_kode_wilayah_kabupaten.csv', index=False)
print('kode_wilayah_kabupaten: ', len(kode_wilayah_kabupaten))
print(f'get kab/kota done in: {time.time() - start_kab_kota} seconds')


"""## get kode wilayah `Kecamatan`"""
start_kecamatan = time.time()
print('getting kecamatan')

def get_kecamatan(kode_wilayah_kabupaten):
    for kabupaten in kode_wilayah_kabupaten:
        retry_count=0
        while retry_count < max_retries:
            try:
                kabupaten = kabupaten.strip()
                kec_api = f'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=2&kode_wilayah={kabupaten}&semester_id=20231'
                kecamatan = pd.read_json(kec_api)
                kecamatan = kecamatan.sort_values('kode_wilayah')
                kecamatan = kecamatan['kode_wilayah']
                kode_wilayah_kecamatan = kode_wilayah_kecamatan._append(kecamatans, ignore_index=True)
                break
            except Exception as e:
                print(f'kode kabupaten: {kabupaten}, error: {e}, retry: {retry_count}')
                if retry_count == 2:
                    print(f'kecamatan with kabupaten code {kabupaten} was failed to be get')
                retry_count += 1
                time.sleep(3)
    return None

def main_get_kode_kecamatan(kode_wilayah_kabupaten):
    threads = []
    kabupaten_batches = np.array_split(kode_wilayah_kabupaten, num_threads)
    for t in range(num_threads):
        thread = threading.Thread(target=get_kecamatan, args=(kabupaten_batches[t],))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    return None


kode_wilayah_kabupaten = pd.read_csv('dataset\kode_wilayah_kabupaten.csv')
kode_wilayah_kabupaten = kode_wilayah_kabupaten['kode_wilayah_kabupaten'][:12]
kode_wilayah_kabupaten = kode_wilayah_kabupaten.astype(str).str.zfill(6)
main_get_kode_kecamatan(kode_wilayah_kabupaten)

kode_wilayah_kecamatan = pd.concat(kode_wilayah_kecamatan, ignore_index=True)
kode_wilayah_kecamatan = kode_wilayah_kecamatan.astype(str).str.zfill(6)
df_kode_wilayah_kecamatan = pd.DataFrame({'kode_wilayah_kecamatan': kode_wilayah_kecamatan})
df_kode_wilayah_kecamatan.to_csv('./dataset/trial_kode_wilayah_kecamatan_thread.csv', index=False)


"""## get semester list"""
#contoh page kecamatan kemayoran. untuk ngambil tag select semester
page_url = 'https://dapo.kemdikbud.go.id/pd/3/016006'
page = driver.get(page_url)
soup = BeautifulSoup(driver.page_source, 'html.parser')

#get periode for rekapitulasi each semester
semester_dropdown = soup.find(id='selectSemester').find_all('option')[1:]
semester_lists = [semester.get('value') for semester in semester_dropdown]
semester_lists = [semester_lists[0]]

print('semester lists ', semester_lists)

semester_titles = [semester.get_text().strip() for semester in semester_dropdown]
print('semester_titles ', semester_titles)


"""## generate rekapitulasi urls in kecamatan level"""
start_rekap = time.time()
kode_wilayah_kecamatan = pd.read_csv('dataset\kode_wilayah_kecamatan_thread.csv')
kode_wilayah_kecamatan = kode_wilayah_kecamatan['kode_wilayah_kecamatan']
kode_wilayah_kecamatan = kode_wilayah_kecamatan.astype(str).str.zfill(6)

rekapitulasi_urls = ['https://dapo.kemdikbud.go.id/rekap/progresSP?id_level_wilayah=3&kode_wilayah={0}&semester_id={1}'.format(kode, semester) for kode, semester in itertools.product(kode_wilayah_kecamatan, semester_lists)]
rekapitulasi_urls = pd.DataFrame({'urls': rekapitulasi_urls})
rekapitulasi_urls.to_csv('./dataset/rekapitulasi_urls.csv', index=False)

print('rekapitulasi_urls: ', len(rekapitulasi_urls))
print(f'generate rekapitulasi url done in: {time.time() - start_rekap} seconds')

"""## get rekapitulasi secara multithreading"""
start_rekapitulasi = time.time()
print('getting rekapitulasi')

def get_school_recap(urls):
    for url in urls:
        retry_count=0
        while retry_count < max_retries:
            try:
                rekapitulasi = pd.read_json(url)
                rekapitulasi_list.append(rekapitulasi)
                break
            except Exception as e:
                print(f'url: {url}, error: {e}, retry: {retry_count}')
                if retry_count == 2:
                    print(f'rekapitulasi with url: {url} was failed to be get')
                    failed_rekapitulasi_list.append(url)
                retry_count += 1
                time.sleep(3)
    return None

def main_get_school_rekapitulasi(rekapitulasi_urls_partition):
    threads = []
    rekapitulasi_batches = np.array_split(rekapitulasi_urls_partition, num_threads)
    for t in range(num_threads):
        thread = threading.Thread(target=get_school_recap, args=(rekapitulasi_batches[t],))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    return None


rekapitulasi_list = []
failed_rekapitulasi_list = []

rekapitulasi_urls = pd.read_csv('dataset\\rekapitulasi_urls_thread_20231.csv')
rekapitulasi_urls = rekapitulasi_urls['urls']
rekapitulasi_urls_partition = np.array_split(rekapitulasi_urls, 10)

for i in range(0, len(rekapitulasi_urls_partition)):
    main_get_school_rekapitulasi(rekapitulasi_urls_partition[i])

rekapitulasi_result = pd.concat(rekapitulasi_list, ignore_index=True)
rekapitulasi_result.to_csv('./result-dataset/rekapitulasi_result_20231.csv', index=False)

print('rekapitulasi len: ', len(rekapitulasi_result))
print(f'get rekapitulasi done in: {time.time() - start_rekapitulasi} seconds')