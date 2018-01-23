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
num_volumes = 488
hathi_domain = 'https://babel.hathitrust.org'
num_threads = 150  # Hathi network can download 100+ pages between errors, so 100+ threads will maximise concurrent page download speed
volumes_dir = 'volumes'
volumes_fieldnames = ['retreived', 'url', 'name',
                      'period', 'session', 'downloaded', 'processed']


class Volume:
    """docstring for Volume."""

    def __init__(self, name, url, period, session, row=None):
        self.row = row
        self.name = name
        self.url = url
        self.period = period
        self.session = session
        self.pages = []


def get_volume_meta():
    filename = 'hathivolumeURLs.csv'

    print('Getting volume URLs:\n')

    volumes = []
    count = 0
    if Path(filename).exists():
        with open(filename, 'r') as url_file:
            for row in csv.DictReader(url_file):
                count += 1
                if not row['downloaded']:
                    print('Volume', row['name'], row['url'])
                    volumes.append(
                        Volume(row['name'], row['url'], row['period'], row['session'], count))

    if count < num_volumes:
        if not count:
            with open(filename, 'w') as url_file:
                writer = csv.DictWriter(url_file, volumes_fieldnames)
                writer.writeheader()
        with open(filename, 'a') as url_file:
            writer = csv.DictWriter(url_file, volumes_fieldnames)
            for row in scrape_volume_urls(count):
                writer.writerow(row)
                count += 1
                print('Volume {}; {}\n{}\n{}\n'.format(
                    row['name'], row['period'], row['session'], row['url']))
                volumes.append(
                    Volume(row['name'], row['url'], row['period'], row['session'], row=count))

    print('\nCollected Hathi volume URLs after {}'.format(get_rate(start_time)))
    print('{}/{} ({}%) volumes downloaded\n'.format(count - len(volumes),
                                                    count, round(100 * (1 - len(volumes) / count), 2)))

    return volumes


def scrape_volume_urls(count):

    volume_directory = download_soup(
        hansard_url).select('.wikitable')[0]('tr')[count + 1:num_volumes + 1]

    results = None
    with ThreadPool(num_threads) as pool:
        results = pool.map(scrape_volume_url, volume_directory)

    return results


def scrape_volume_url(tr):
    # print(type(tr))
    row = {}
    tds = tr('td')
    switch = None
    breaker = False
    name, retreived, href, period, session = '', '', '', '', ''
    for td in tds:
        if td.a:
            for a in td('a'):
                # print(type(tr))
                if a.string:
                    row['name'] = a.string.strip()
                    row['url'] = download_soup(a['href']).select(
                        '.accessOverview')[0].p.a['href']
                    row['retreived'] = datetime.now()
        else:
            if switch:
                row['session'] = td.get_text().strip()
            else:
                row['period'] = td.string.strip()
                switch = True
    print('Got link:', row)
    return row


def download_volumes():
    volumes = get_volume_meta()
    if not Path(volumes_dir).exists():
        mkdir(volumes_dir)

    t = time.time()
    with ThreadPool(num_threads) as pool:
        results = pool.map(download_volume, volumes)

    print("\n--- {} volumes downloaded in {} ---".format(len(volumes), get_rate(t)))


write_lock = Lock()


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
    condition = True
    if url:
        condition, url, _ = download_page(
            url.replace(hathi_domain, '', 1), pagecount)
    else:
        url = volume.url
    with open(filepath, 'a') as txt_file:
        writer = csv.DictWriter(txt_file, fieldnames)
        while condition:
            pagecount += 1
            condition, url, row = download_page(url, pagecount)
            if row:
                writer.writerow(row)

    # Update the record of volume downloads:
    completion = 1
    with write_lock:
        filename = 'hathivolumeURLs.csv'
        count, rows = 0, []
        with open(filename, 'r') as url_file:
            reader = csv.DictReader(url_file)
            for row in reader:
                count += 1
                if row['downloaded']:
                    completion += 1
                if volume.row == count:
                    row['downloaded'] = True
                rows.append(row)
            completion = round(100 * completion / count, 2)

        with open(filename, 'w') as url_file:
            writer = csv.DictWriter(url_file, volumes_fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        print('Volume {} complete! Downloading {}{} complete\n'.format(
            name, completion, '%'))


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


sleep_lock = Lock()
responsive_lock = Lock()
responsive = True
tries = 0
count_lock = Lock()
total_pages_processed = 0
interval_pages_processed = 0


def download_soup(url):
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


def main():
    try:
        download_volumes()
        # process_volumes()
        print('Corpus compilation successful\n')
    except Exception as e:
        raise e
    finally:
        print("\n--- Job took {} ---".format(get_rate(start_time)))
        # print('Looped through {} strings while processing {}\n'.format(
        # most_loops, longest_day))


start_time = time.time()


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
