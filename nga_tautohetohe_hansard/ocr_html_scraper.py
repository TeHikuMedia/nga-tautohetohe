# import libraries
import csv
import time
from os import mkdir
from pathlib import Path
from multiprocessing.dummy import Pool as ThreadPool, Lock
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs
from datetime import datetime
from taumahi import *

hansard_url = 'https://www.parliament.nz/en/pb/hansard-debates/historical-hansard/'
hathi_domain = 'https://babel.hathitrust.org'
volumeindex_filename = 'hansardvolumeindex.csv'
volumeindex_fieldnames = ['retrieved', 'url', 'name', 'period', 'session', 'format', 'downloaded', 'processed']
volumes_dir = '1854-1987'

# Volume counts:
num_volumes = 482 + 6 - 4  # 4 sources contain 2 volumes combined
complete = 0

# Hathi network can download 100+ pages between errors,
# so 100+ threads will maximise concurrent page download speed
num_threads = 100
write_lock = Lock()
sleep_lock = Lock()
count_lock = Lock()
responsive_lock = Lock()
responsive = True
total_pages_processed = interval_pages_processed = tries = 0
start_time = time.time()


def get_volume_meta():
    print('Getting volume URLs:')
    global complete
    incomplete = 0

    # Check to see if all volume urls have been retrieved and
    # if any volumes have already been downloaded and processed:
    if Path(volumeindex_filename).exists():
        with write_lock:
            for row in read_index_rows():
                if row['downloaded']:
                    complete += 1
                else:
                    incomplete += 1
                    print('Have link to volume:', row['name'])
                    yield row
    total = complete + incomplete
    if not total:
        with write_lock, open(volumeindex_filename, 'w', newline='', encoding='utf8') as v_index:
            writer = csv.DictWriter(v_index, volumeindex_fieldnames)
            writer.writeheader()

    # Get remaining urls if they haven't been acquired yet:
    if total < num_volumes:
        for row in scrape_volume_urls(total):
            while True:
                with write_lock, open(volumeindex_filename, 'a', newline='', encoding='utf8') as v_index:
                    writer = csv.DictWriter(v_index, volumeindex_fieldnames)
                    writer.writerow(row)
                    break
            yield row
    print(f'Collected Hathi volume URLs after {get_rate(start_time)}')


def read_index_rows():
    while True:
        rows = []
        with open(volumeindex_filename, 'r', newline='', encoding='utf8') as url_file:
            reader = csv.DictReader(url_file)
            for row in reader:
                if not (row['name'].isdigit() and int(row['name']) > 482):
                    rows.append(row)
            return rows


def scrape_volume_urls(count):
    if count > 69 + 6:
        count += 1
        if count > 96 + 6:
            count += 1
            if count > 135 + 6:
                count += 1
                if count > 145 + 6:
                    count += 1

    # Scrape meta data from table of Hansard volumes:
    volume_directory = download_soup(hansard_url).select('.wikitable')[0]('tr')[count + 1:num_volumes + 1]

    previous_result = None
    with ThreadPool(num_volumes - count if num_threads > num_volumes - count else num_threads) as pool:
        for result in pool.imap(scrape_volume_url, volume_directory):
            # Several volumes are combined, therefore check for duplicate url before yielding value:
            if previous_result:
                if previous_result['url'] == result['url']:
                    result['name'] = f'{previous_result["name"]} - {result["name"]}'
                    result['session'] = f'{previous_result["session"]} & {result["session"]}'
                    result['period'] = f'{previous_result["period"]} & {result["period"]}'
                else:
                    yield previous_result
            previous_result = result
        yield previous_result


def scrape_volume_url(tr):
    # Scrape data from each cell of each row of Hansard table:
    row = {'format': 'OCR'}
    row_cells = tr('td')
    switch = ''
    for cell in row_cells:
        if cell.a:
            for a in cell('a'):
                if a.string:
                    row['name'] = a.string.strip()
                    row['url'] = hathi_domain + download_soup(a['href']).select('.accessOverview')[0].p.a['href']
                    row['retrieved'] = datetime.now()
        else:
            if switch:
                row['session'] = cell.get_text().strip()
            else:
                row['period'] = cell.string.strip()
                switch = True
    print('Got link to volume:', row['name'])
    return row


def download_volumes():
    if not Path(volumes_dir).exists():
        mkdir(volumes_dir)

    t = time.time()
    # Get list of volumes that are not finished downloading, then download:
    volume_list = get_volume_meta()
    with ThreadPool(num_volumes - complete if num_threads > num_volumes - complete else num_threads) as pool:
        count = 0
        for result in pool.imap_unordered(download_volume, volume_list):
            count += 1
        print(f"--- {count} volumes downloaded in {get_rate(t)} ---")


def download_volume(volume):
    name = volume['name']
    print(f'Downloading volume {name}')

    # Check to see how much of the volume has been downloaded
    filepath = f'{volumes_dir}/{name}.csv'
    url = ''
    pagecount = 0
    fieldnames = []
    if Path(filepath).exists():
        with open(filepath, 'r', newline='', encoding='utf8') as txt_file:
            reader = csv.DictReader(txt_file)
            fieldnames = reader.fieldnames
            c = 0
            try:
                for row in reader:
                    url = row['url']
                    pagecount = int(row['page'])
                    c += 1
            except Exception as exception:
                print(filepath, c)
                if not c:
                    with open(filepath, 'w', newline='', encoding='utf8') as txt_file:
                        fieldnames = ['retrieved', 'url', 'page', 'text']
                        writer = csv.DictWriter(txt_file, fieldnames)
                        writer.writeheader()
                else:
                    raise exception
    else:
        with open(filepath, 'w', newline='', encoding='utf8') as txt_file:
            fieldnames = ['retrieved', 'url', 'page', 'text']
            writer = csv.DictWriter(txt_file, fieldnames)
            writer.writeheader()

    # Download and save remaining volume pages:
    more_pages = True
    if url:
        more_pages, url, _ = download_page(url.replace(hathi_domain, '', 1), pagecount)
    else:
        url = volume['url'].replace(hathi_domain, '', 1)
    while more_pages:
        pagecount += 1
        more_pages, url, row = download_page(url, pagecount)
        if row:
            while True:
                with open(filepath, 'a', newline='', encoding='utf8') as txt_file:
                    writer = csv.DictWriter(txt_file, fieldnames)
                    writer.writerow(row)
                    break

    # Update the record of volume downloads:
    with write_lock:
        completion = 1
        rows = []
        for row in read_index_rows():
            if row['downloaded']:
                completion += 1
            elif name == row['name']:
                row['downloaded'] = True
            rows.append(row)
        percent = round(100 * completion / num_volumes, 2)

        while True:
            with open(volumeindex_filename, 'w', newline='', encoding='utf8') as url_file:
                writer = csv.DictWriter(url_file, volumeindex_fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f'Volume {name} complete! Downloading {percent}{"%"} ({completion}/{num_volumes}) complete at',
                  f'{datetime.now()} after {get_rate(start_time)}\n')
            break


def download_page(url, page):
    soup = download_soup(f'{hathi_domain}{url}')

    row = {}
    page_soup = soup.find(id='mdpPage')
    text = page_soup.find(class_='Text')
    if text:
        row = {'retrieved': datetime.now(), 'url': url, 'page': page, 'text': text.string}

    url = ''
    anchors = page_soup('a')
    if page == 1:
        for a in anchors:
            if a.get_text().strip() == 'Next Page':
                url = a['href']
    else:
        url = anchors[1]['href']

    return url != '#top', url, row


def download_soup(url):
    # Retrieves a url and returns soup from parsed HTML.
    # This method is threadsafe and from a single client IP it is useful for
    # bombarding a domain with hundreds of queries per second if their
    # server allows it and puts threads to sleep if the server times out
    # until the server is ready to start receiving requests again.
    global total_pages_processed, responsive, tries, interval_pages_processed

    with responsive_lock:
        while not responsive:
            time.sleep(0.5)
    while True:
        try:
            # download then parse the page and return if successful
            soup = bs(urlopen(url), 'html.parser')
            responsive = True

            with count_lock:
                if tries > 0:
                    # reset attempt counter
                    interval_pages_processed = tries = 0
                    print(f'Downloaded {total_pages_processed} pages in {get_rate(start_time)} at',
                          f'{round(total_pages_processed / (time.time() - start_time), 2)} p/s')
                total_pages_processed += 1
                interval_pages_processed += 1

            return soup

        except Exception as exception:
            responsive = False
            with sleep_lock:
                if not responsive:
                    print(exception, f'\n{interval_pages_processed} pages downloaded since last error')
                    time.sleep(1)
                    if not responsive:
                        time.sleep(4)
                        tries += 1
                        print(f'Attempting to retrieve: {url}')


def main():
    try:
        print('Downloading volumes 1854-1987:')
        download_volumes()
        print('Hansard download successful.')

    except Exception as exception:
        raise exception
    finally:
        print(f"--- Job took {get_rate(start_time)} ---\n")


def get_rate(t):
    m, s = divmod(time.time() - t, 60)
    s = int(s)
    h, m = divmod(m, 60)
    if m:
        m = int(m)
        if h:
            return f'{int(h)} hours {m} minutes {s} seconds'
        else:
            return f'{m} minutes {s} seconds'
    return f'{s} seconds'


if __name__ == '__main__':
    main()
