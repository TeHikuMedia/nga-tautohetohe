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
volumes_dir = 'volumes'


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

    if count < 480:
        with open(filename, 'w') as url_file:
            writer = csv.writer(url_file)
            writer.writerow(
                ['retreived', 'url', 'name', 'period', 'session', 'downloaded'])
            volumes = scrape_volume_urls(writer)

    print('\nCollected Hathi volume URLs after {}'.format(get_rate(start_time)))
    print('{}/{} ({}%) volumes downloaded\n'.format(len(volumes),
                                                    count, round(100 * (1 - len(volumes) / count), 2)))

    return volumes


def scrape_volume_urls(writer):

    volumes = []
    volume_directory = bs(urlopen(hansard_url), 'html.parser').select(
        '.wikitable')[0]('tr')[1:]

    breaker = None
    count = 0
    for tr in volume_directory:
        tds = tr('td')
        switch = None
        name, retreived, href, period, session = '', '', '', '', ''
        for td in tds:
            if td.a:
                for a in td('a'):
                    if a.string:
                        name = a.string.strip()
                        breaker = name == '482'
                        href = bs(urlopen(a['href']), 'html.parser').select(
                            selector='.accessOverview')[0].p.a['href']
                        retreived = datetime.now()
            else:
                if switch:
                    session = td.get_text().strip()
                else:
                    period = td.string.strip()
                    switch = True
        print('Volume {}; {}\n{}\n{}\n'.format(name, period, session, href))
        count += 1
        writer.writerow([retreived, href, name, period, session])
        volumes.append(Volume(name=name, url=href,
                              period=period, session=session, row=count))
        if breaker:
            break
    return volumes


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
    url = ''
    pagecount = 0
    fieldnames = []

    print('Downloading volume {}'.format(name))

    filepath = '{}/{}.csv'.format(volumes_dir, name)
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
            writer = csv.writer(txt_file)
            writer.writerow(fieldnames)

    condition = True
    if url:
        condition, url, _ = download_page(
            url.replace(hathi_domain, '', 1), name, pagecount)
    else:
        url = volume.url

    with open(filepath, 'a') as txt_file:
        writer = csv.DictWriter(txt_file, fieldnames=fieldnames)
        while condition:
            pagecount += 1
            condition, url, row = download_page(url, name, pagecount)
            if row:
                writer.writerow(row)
                print('Volume {}, page {}'.format(name, row['page']))

    completion = 1
    with write_lock:
        filename = 'hathivolumeURLs.csv'
        count, rows = 0, []
        with open(filename, 'r') as url_file:
            for row in csv.DictReader(url_file):
                count += 1
                if row['downloaded']:
                    completion += 1
                if volume.row == count:
                    row['downloaded'] = True
                rows.append(row)
            completion = round(100 * completion / count, 2)

        with open(filename, 'w') as url_file:
            writer = csv.DictWriter(url_file, fieldnames=[
                                    'retreived', 'url', 'name', 'period', 'session', 'downloaded'])
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    print('Volume {} complete! Downloading {}%% complete\n'.format(name, completion))
    # p = Process()


num_threads = 150  # Network can download 100+ pages between errors, so 100+ threads will maximise concurrent page download speed
sleep_lock = Lock()
responsive_lock = Lock()
responsive = True
tries = 0
count_lock = Lock()
total_pages_processed = 0
interval_pages_processed = 0


def download_page(url, name, page):
    url = '{}{}'.format(hathi_domain, url)
    soup = None
    global total_pages_processed, responsive, tries, interval_pages_processed

    with responsive_lock:
        while not responsive:
            time.sleep(0.5)
    while True:
        try:
            # parse the page and break if successful
            soup = bs(urlopen(url), 'html.parser')
            responsive = True
            break
        except Exception as e:
            responsive = False
            with sleep_lock:
                if not responsive:
                    print('Downloaded {} since last error -'.format(
                        interval_pages_processed), e)
                    time.sleep(1)
                    if not responsive:
                        time.sleep(4)
                        tries += 1
                        print(
                            'Attempt {} - retrieving: volume {}, page {}'.format(tries, name, page))

    with count_lock:
        if tries > 0:
            # reset attempt counter
            interval_pages_processed = 0
            tries = 0
            print('Downloaded {} pages in {} at {} p/s'.format(
                total_pages_processed, get_rate(start_time), round(total_pages_processed / (time.time() - start_time), 2)))
        total_pages_processed += 1
        interval_pages_processed += 1

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
