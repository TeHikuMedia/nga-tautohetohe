# import libraries
import csv
import re
import time
from os import mkdir
from pathlib import Path
from multiprocessing.dummy import Pool as ThreadPool
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs
from datetime import datetime
from taumahi import *

hansard_url = 'https://www.parliament.nz/en/pb/hansard-debates/historical-hansard/'
hathi_domain = 'https://babel.hathitrust.org'
volumes_dir = 'volumes'


class Volume:
    """docstring for Volume."""

    def __init__(self, name, url, period, session):
        self.name = name
        self.url = url
        self.period = period
        self.session = session
        self.pages = []


def get_volume_meta():
    filename = 'hathivolumeURLs.csv'

    print('Getting volume URLs:\n')

    volumes = []

    if Path(filename).exists():
        with open(filename, 'r') as url_file:
            for row in csv.DictReader(url_file):
                print('Volume', row['name'], row['url'])
                volumes.append(
                    Volume(row['name'], row['url'], row['period'], row['session']))

    if len(volumes) < 487:
        with open(filename, 'w') as url_file:
            writer = csv.writer(url_file)
            writer.writerow(
                ['retreived', 'url', 'name', 'period', 'session'])
            volumes = scrape_volume_urls(writer)

    print('\nCollected Hathi volume URLs after {}\n'.format(get_rate()))

    return volumes


def scrape_volume_urls(writer):

    volumes = []
    volume_directory = bs(urlopen(hansard_url), 'html.parser').select(
        '.wikitable')[0]('tr')[1:]

    breaker = None

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
        writer.writerow([retreived, href, name, period, session])
        volumes.append(Volume(name=name, url=href,
                              period=period, session=session))
        if breaker:
            break
    return volumes


def download_volumes():
    volumes = get_volume_meta()
    if not Path(volumes_dir).exists():
        mkdir(volumes_dir)

    pool = ThreadPool(6)
    results = pool.map(download_volume, volumes)


def download_volume(volume):
    filepath = '{}/{}.csv'.format(volumes_dir, volume.name)
    rowcount = 0

    if Path(filepath).exists():
        with open(filepath, 'r') as txt_file:
            for row in csv.DictReader(txt_file):
                rowcount += 1

    if rowcount < 100:
        with open(filepath, 'w') as txt_file:
            writer = csv.writer(txt_file)
            writer.writerow(['retreived', 'url', 'name', 'page', 'text'])
            url = volume.url
            pagecount = 0
            while True:
                pagecount += 1
                url = '{}{}'.format(hathi_domain, url)

                # reset attempt counter
                tries = 1
                soup = None
                while True:
                    try:
                        # parse the page and break if successful
                        soup = bs(urlopen(url), 'html.parser')
                        tries += 1
                        break
                    except Exception as e:
                        # sleep briefly if there's an error
                        tries += 1
                        print('Attempt {} retrieving: {}\n'.format(tries, url))
                        time.sleep(4)

                page_soup = soup.find(id='mdpPage')
                text = page_soup.find(class_='Text')
                if text:
                    writer.writerow(
                        [datetime.now(), url, volume.name, pagecount, text.string])
                    print('Volume {}, page {}'.format(
                        volume.name, pagecount))

                url = page_soup('a')[1]['href']
                if url == '#top':
                    break


class HansardTuhingaScraper:
    def __init__(self, doc_url):
        ''' Set up our tuhituhi CorpusCollector with basic params '''
        self.doc_url = doc_url
        self.hanga_hupo()

    def hanga_hupo(self):
        # query the website and parse the returned html using beautiful soup

        doc_id = self.doc_url.split('/')[6]
        alternative_URL = '{}{}'.format(hansard_meta_url, doc_id)
        get_stuff = ''
        exception_flag = None

        try:
            get_stuff = urlopen('{}{}'.format(
                hansard_url, self.doc_url))
        except Exception as e:
            print(e, '\nTrying alternative URL...')
            try:
                get_stuff = urlopen(alternative_URL)
                exception_flag = True
                print('\nSuccess!\n')
            except Exception as e:
                raise Exception(e, '\nCould not find data')

        self.soup = bs(get_stuff, 'html.parser')

        self.retreived = datetime.now()

        if exception_flag:
            self.kōrero_hupo = self.soup.find('div', attrs={'class': 'section'}).select(
                'div.section > div.section')
        elif re.match(r'\d', doc_id):
            self.kōrero_hupo = self.soup.select('div.Hansard > div')
        else:
            self.kōrero_hupo = self.soup.find_all(
                'div', attrs={'class': 'section'})

        # Make soup from hansard metadata
        meta_url = '{}{}'.format(alternative_URL, '/metadata')
        self.metasoup = bs(urlopen(meta_url), 'html.parser').table

    def horoi_transcript_factory(self):

        meta_entries = self.metasoup.find_all('td')

        doc_url = '{}{}'.format(hansard_url, self.doc_url)
        wā = meta_entries[1].get_text()
        title = meta_entries[0].get_text()

        transcripts = []
        totals = [0, 0, 0]
        awaiting_teReo = None
        section_count = 0

        print('\n{}\n'.format(doc_url))

        for section in self.kōrero_hupo:

            section_count += 1
            paragraph_count = 0
            ingoa_kaikōrero = ''

            p_list = section.find_all('p')
            print('Paragraphs =', len(p_list))

            for paragraph in p_list:

                strong_tags = paragraph.find_all('strong')

                flag = False

                for strong in strong_tags:
                    string = strong.extract().string
                    if not flag and string and re.search(r'[a-zA-Z]{5,}', string):
                        ingoa_kaikōrero = string.strip()
                        flag = True

                kōrero = paragraph.get_text(strip=True)
                check = None
                if re.match(r'\[.*\]', kōrero):
                    if re.match(r'\[Authorised Te Reo text', kōrero):
                        check = True
                        awaiting_teReo = True
                    else:
                        continue

                if flag:
                    p = kōrero.split(':', 1)[-1].strip()
                    if p:
                        kōrero = p

                if re.search(r'[a-zA-Z]', kōrero):
                    paragraph_count += 1

                    save_corpus, numbers = kupu_ratios(kōrero)

                    for i in range(len(totals)):
                        totals[i] = totals[i] + numbers[i]

                    if save_corpus or check:
                        print('{}: {}\nsection {}, paragraph {}, Maori = {}%\nname:{}\n{}\n'.format(
                            wā, title, section_count,
                            paragraph_count, numbers[3], ingoa_kaikōrero, kōrero))
                        transcripts.append([doc_url, wā, title, section_count, paragraph_count,
                                            ingoa_kaikōrero] + numbers + [clean_whitespace(kōrero)])
        print('Time:', self.retreived)
        doc_record = [self.retreived, self.doc_url, wā, title] + totals + \
            [get_percentage(totals[0], totals[1], totals[2]), awaiting_teReo]
        return transcripts, doc_record


def aggregate_hansard_corpus(doc_urls):
    transcripts = []

    corpusfilename = 'historicalhansardcorpus.csv'
    recordfilename = 'historicalhansardindex.csv'

    record_list = []
    waiting_for_reo_list = []

    if Path(recordfilename).exists():
        with open(recordfilename, 'r') as record_file:
            rowcount = 0
            for row in csv.DictReader(record_file):
                record_list.append(row)
                if row['awaiting authorised reo'] is True:
                    waiting_for_reo_list.append(rowcount)
                rowcount += 1
    else:
        with open(recordfilename, 'w') as record_file:
            head_writer = csv.writer(record_file)
            head_writer.writerow([
                'Date retreived',
                'Hansard document url',
                'Wā',
                'Title',
                'Te Reo length',
                'Ambiguous length',
                'Other length',
                'Is Māori (%)',
                'Awaiting authorised reo'
            ])

    if not Path(corpusfilename).exists():
        with open(corpusfilename, 'w') as corpus:
            head_writer = csv.writer(corpus)
            head_writer.writerow([
                'Hansard document url',
                'Wā',
                'Title',
                'Section number',
                'Utterance number',
                'Ingoa kaikōrero',
                'Te Reo length',
                'Ambiguous length',
                'Other length',
                'Is Māori (%)',
                'Kōrero'
            ])

    remaining_urls = []

    if record_list:
        last_record_url = record_list[-1]['Hansard document url']
        remaining_urls = doc_urls[doc_urls.index(last_record_url) + 1:]
    else:
        remaining_urls = doc_urls

    with open(recordfilename, 'a') as record:
        with open(corpusfilename, 'a') as kiwaho:
            record_csv = csv.writer(record)
            hansard_csv = csv.writer(kiwaho)

            for doc_url in remaining_urls:
                corpus_writer(doc_url, record_csv, hansard_csv)

                print('---\n')


def corpus_writer(doc_url, record_csv, hansard_csv):
    transcripts, doc_record = HansardTuhingaScraper(
        doc_url).horoi_transcript_factory()

    record_csv.writerow(doc_record)
    if transcripts:
        for transcript in transcripts:
            hansard_csv.writerow(transcript)


def main():
    try:
        download_volumes()
        # process_volumes()
        print('Corpus compilation successful\n')
    except Exception as e:
        raise e
    finally:
        print("\n--- Job took {} ---".format(get_rate()))
        # print('Looped through {} strings while processing {}\n'.format(
        # most_loops, longest_day))


start_time = time.time()
longest_day = ''
most_loops = 0


def get_rate():
    m, s = divmod(time.time() - start_time, 60)
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
