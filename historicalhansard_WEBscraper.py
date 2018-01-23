# import libraries
import csv
import re
import time
from os import mkdir
from pathlib import Path
from multiprocessing.dummy import Process, Pool as ThreadPool, Lock
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs
from datetime import datetime
from taumahi import *

hansard_url = 'https://www.parliament.nz/en/pb/hansard-debates/historical-hansard/'
hathi_domain = 'https://babel.hathitrust.org'
index_filename = 'hathivolumeURLs.csv'
volumes_dir = 'volumes'
volumes_fieldnames = ['retreived', 'url', 'name',
                      'period', 'session', 'downloaded', 'processed']
num_volumes = 488
num_threads = 150  # Hathi network can download 100+ pages between errors, so 100+ threads will maximise concurrent page download speed
write_lock = Lock()
sleep_lock = Lock()
responsive_lock = Lock()
responsive = True
tries = 0
count_lock = Lock()
total_pages_processed = 0
interval_pages_processed = 0
start_time = time.time()


class Volume:
    """docstring for Volume."""

    def __init__(self, name, url, period, session):
        self.name = name
        self.url = url
        self.period = period
        self.session = session
        self.pages = []


def get_volume_meta():
    print('Getting volume URLs:\n')
    volumes = []
    complete = 0
    incomplete = 0

    # Check to see if all volume urls have been retreived and
    # if any volumes have already been downloaded and processed:
    if Path(filename).exists():
        with write_lock:
            for row in read_index_rows():
                if row['downloaded']:
                    complete += 1
                else:
                    incomplete += 1
                    print('Have link to volume:', row['name'])
                    yield Volume(row['name'], row['url'], row['period'], row['session'])
            if not complete:
                with open(index_filename, 'w') as url_file:
                    writer = csv.DictWriter(url_file, volumes_fieldnames)
                    writer.writeheader()

    # Get remaining urls if they haven't been acquired yet:
    if complete + incomplete < num_volumes:
        for row in scrape_volume_urls(total):
            while True:
                with write_lock, open(index_filename, 'a') as url_file:
                    writer = csv.DictWriter(url_file, volumes_fieldnames)
                    writer.writerow(row)
                    break
            yield Volume(row['name'], row['url'], row['period'], row['session'])

        # Original script before incorporating pool.imap with the 'yield'statement:
        # urls = scrape_volume_urls(total)
        # with open(filename, 'a') as url_file:
        #     writer = csv.DictWriter(url_file, volumes_fieldnames)
        #     for row in urls:
        #         writer.writerow(row)
        #         yield Volume(row['name'], row['url'], row['period'], row['session'])

    print('\nCollected Hathi volume URLs after {}'.format(get_rate(start_time)))
    print('{}/{} ({}%) volumes downloaded\n'.format(complete,
                                                    num_volumes, round(100 * (complete / num_volumes), 2)))


def read_index_rows(arg):
    while True:
        rows = []
        with open(index_filename, 'r') as url_file:
            reader = csv.DictReader(url_file)
            for row in reader:
                rows.append(row)
            return rows


def scrape_volume_urls(count):
    # Scrape meta data from table of Hansard volumes
    volume_directory = download_soup(
        hansard_url).select('.wikitable')[0]('tr')[count + 1:num_volumes + 1]

    results = None
    with ThreadPool(num_threads) as pool:
        for result in pool.imap(scrape_volume_url, volume_directory):
            yield result


def scrape_volume_url(tr):
    # Scrape data from each cell of each row of Hansard table
    row = {}
    row_cells = tr('td')
    switch = None
    breaker = False
    name, retreived, href, period, session = '', '', '', '', ''
    for cell in row_cells:
        if cell.a:
            for a in cell('a'):
                # print(type(tr))
                if a.string:
                    row['name'] = a.string.strip()
                    row['url'] = download_soup(a['href']).select(
                        '.accessOverview')[0].p.a['href']
                    row['retreived'] = datetime.now()
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
    with ThreadPool(num_threads) as pool:
        for result in pool.imap_unordered(download_volume, get_volume_meta()):
            pass  # yeild result: start text processing script thread

    print("\n--- {} volumes downloaded in {} ---".format(len(volumes), get_rate(t)))


def download_volume(volume):
    name = volume.name
    filepath = '{}/{}.csv'.format(volumes_dir, name)

    print('Downloading volume {}'.format(name))

    # Check to see how much of the volume has been downloaded
    url = ''
    pagecount = 0
    fieldnames = []
    if Path(filepath).exists():
        with open(filepath, 'r') as txt_file:
            reader = csv.DictReader(txt_file)
            fieldnames = reader.fieldnames
            for row in reader:
                url = row['url']
                pagecount = int(row['page'])
    else:
        with open(filepath, 'w') as txt_file:
            fieldnames = ['retreived', 'url', 'page', 'text']
            writer = csv.DictWriter(
                txt_file, fieldnames)
            writer.writeheader()

    # Download and save remaining volume pages:
    more_pages = True
    if url:
        more_pages, url, _ = download_page(
            url.replace(hathi_domain, '', 1), pagecount)
    else:
        url = volume.url
    while more_pages:
        pagecount += 1
        condition, url, row = download_page(url, pagecount)
        if row:
            while True:
                with open(filepath, 'a') as txt_file:
                    writer = csv.DictWriter(txt_file, fieldnames)
                    writer.writerow(row)
                    break

    # Update the record of volume downloads:
    with write_lock:
        completion = 1
        filename = 'hathivolumeURLs.csv'
        rows = []
        for row in get_index_rows():
            if row['downloaded']:
                completion += 1
            elif volume.name == row['name']:
                row['downloaded'] = True
            rows.append(row)
        completion = round(100 * completion / num_volumes, 2)

        while True:
            with open(filename, 'w') as url_file:
                writer = csv.DictWriter(url_file, volumes_fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
            print('Volume {} complete! Downloading {}{} complete\n'.format(
                name, completion, '%'))
            break


def download_page(url, page):
    soup = download_soup('{}{}'.format(hathi_domain, url))

    row = {}
    page_soup = soup.find(id='mdpPage')
    text = page_soup.find(class_='Text')
    if text:
        row = {'retreived': datetime.now(), 'url': url, 'page': page,
               'text': text.string}

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
                    interval_pages_processed = 0
                    tries = 0
                    print('Downloaded {} pages in {} at {} p/s'.format(
                        total_pages_processed, get_rate(start_time), round(total_pages_processed / (time.time() - start_time), 2)))
                total_pages_processed += 1
                interval_pages_processed += 1

            return soup

        except Exception as e:
            responsive = False
            with sleep_lock:
                if not responsive:
                    print(e, '\n{} pages downloaded since last error'.format(
                        interval_pages_processed))
                    time.sleep(1)
                    if not responsive:
                        time.sleep(4)
                        tries += 1
                        print('Attempting to retrieve: {}'.format(url))


def process_csv(arg):
    pass


def main():
    try:

        t = time.time()
        with ThreadPool(8) as pool:
            for result in pool.imap_unordered(process_csv, download_volumes()):
                # print(result)
                pass
        print("\n--- {} volumes processed in {} ---".format(len(volumes), get_rate(t)))

        print('Corpus compilation successful\n')

    except Exception as e:
        raise e
    finally:
        print("\n--- Job took {} ---".format(get_rate(start_time)))


def get_rate(t):
    m, s = divmod(time.time() - t, 60)
    s = int(s)
    h, m = divmod(m, 60)
    if m:
        m = int(m)
        if h:
            return '{} hours {} minutes {} seconds'.format(int(h), m, s)
        else:
            return '{} minutes {} seconds'.format(m, s)
    return '{} seconds'.format(s)


if __name__ == '__main__':
    main()
