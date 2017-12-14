# import libraries
import csv
import re
from collections import deque
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs
from datetime import datetime
from taumahi import tatau_tupu

hansard_url = 'https://www.parliament.nz'
hansard_meta_url = '{}{}'.format(hansard_url, '/en/document/')
# doc_url = '{}{}'.format(hansard_doc_url, '123456789')
# meta_url = '{}{}'.format(doc_url, '/metadata')
# print(hansard_url)
# print(hansard_doc_url)
# print(doc_url)
# print(meta_url)


class Transcript:
    def __init__(self, doc_url, wā, title, section, paragraph, ingoa_kaikōrero, heMāori, kōrero_waenga):
        ''' Generate a transcript object with basic params '''
        self.doc_url = doc_url
        self.wā = wā
        self.title = title
        self.section = section
        self.paragraph = paragraph
        self.heMāori = heMāori
        self.ingoa_kaikōrero = ingoa_kaikōrero
        self.kōrero_waenga = kōrero_waenga

    def listify(self):
        return [
            self.doc_url,
            self.wā,
            self.title,
            self.section,
            self.paragraph,
            self.ingoa_kaikōrero,
            self.heMāori,
            self.kōrero_waenga
        ]


class HansardTuhingaScraper:
    def __init__(self, doc_url):
        ''' Set up our tuhituhi CorpusCollector with basic params '''
        self.doc_url = doc_url
        self.hanga_hupo()

    def hanga_hupo(self):
        # query the website and parse the returned html using beautiful soup
        self.soup = bs(urlopen('{}{}'.format(
            hansard_url, self.doc_url)), 'html.parser')

        doc_id = self.doc_url.split('/')[6]

        if re.match(r'\d', doc_id):
            self.kōrero_hupo = self.soup.select('div.Hansard > div')
        else:
            self.kōrero_hupo = self.soup.find_all(
                'div', attrs={'class': 'section'})

        # Make soup from hansard metadata
        meta_url = '{}{}{}'.format(
            hansard_meta_url, doc_id, '/metadata')
        self.metasoup = bs(urlopen(meta_url), 'html.parser').table

    def horoi_transcript_factory(self):

        print(self.doc_url)

        meta_entries = self.metasoup.find_all('td')
        wā = meta_entries[1].get_text()
        title = meta_entries[0].get_text()

        transcripts = []

        teReo_size = 0
        total_size = 0
        awaiting_teReo = None

        section_count = 0

        for section in self.kōrero_hupo:

            section_count += 1
            print('section:', section_count)
            paragraph_count = 0

            ingoa_kaikōrero = ''

            for paragraph in section.find_all('p'):

                print('paragraph: ', paragraph_count)

                strong_tags = paragraph.find_all('strong')

                flag = False

                for strong in strong_tags:
                    string = strong.extract().string
                    if not flag and string and re.search(r'[a-zA-Z]{5,}', string):
                        ingoa_kaikōrero = string.strip()
                        flag = True

                kōrero_waenga = paragraph.get_text(strip=True)

                if flag:
                    # p = re.search(r'(?<=:).*', kōrero_waenga)
                    p = kōrero_waenga.split(':', 1)[-1].strip()
                    if p:
                        kōrero_waenga = p

                if re.search(r'[a-zA-Z]', kōrero_waenga):
                    paragraph_count += 1
                    num_Māori, num_non_Māori = tatau_tupu(paragraph)

                    teReo_size += num_Māori
                    total_size += num_non_Māori

                    heMāori = num_Māori / num_non_Māori

                    save_corpus = heMāori > 50
                    if not save_corpus:
                        save_corpus = re.match(
                            r'\[Authorised Te Reo text', kōrero_waenga)
                        if save_corpus:
                            awaiting_teReo = True

                    if save_corpus:
                        print('{}: {}\nsection {}, paragraph {}, {}%\nname:{}\n{}\n'.format(
                            wā, title, section_count,
                            paragraph_count, heMāori, ingoa_kaikōrero, kōrero_waenga))
                        transcripts.append(Transcript(doc_url='{}{}'.format(hansard_url, self.doc_url),
                                                      wā=wā,
                                                      title=title,
                                                      section=section_count,
                                                      paragraph=paragraph_count,
                                                      ingoa_kaikōrero=ingoa_kaikōrero,
                                                      heMāori=heMāori,
                                                      kōrero_waenga=kōrero_waenga))
        return transcripts, teReo_size, total_size, awaiting_teReo


def scrape_Hansard_URLs():
    list_url = '{}{}'.format(hansard_url, '/en/pb/hansard-debates/rhr/')
    rhr_soup = bs(urlopen(list_url), 'html.parser')
    filename = 'urlindex.csv’

    has_header = False
    doc_url_list = []

    with open(filename, 'r') as url_file:
        has_header = csv.Sniffer().has_header(url_file)
        if has_header:
            for row in csv.DictReader(url_file):
                doc_url_list.append(row['url'])

    if not has_header:
        with open(filename, 'w') as url_file:
            head_writer = csv.writer(url_file)
            head_writer.writerow(['Date retreived', 'url'])

    last_url = doc_url_list[-1]
    new_list = get_new_urls(last_url)

    with open(filename, 'a') as url_file:
        for url in reversed(new_list):
            doc_url_list.append(url[1])
            url_writer.writerow(url)

    return doc_url_list


def get_new_urls(last_url):
    new_list = []
    while True:
        retreivedtime = datetime.now()
        for h2 in rhr_soup.select('ul.hansard__list h2'):
            new_url = h2.a['href']
            if new_url is last_url:
                return new_list
            else:
                print(new_url)
                new_list.append([retreivedtime, new_url])

        next_page = rhr_soup.find(
            'li', attrs={'class', 'pagination__next'})

        if next_page:
            next_url = '{}{}'.format(hansard_url, next_page.find(
                'a')['href'])
            rhr_soup = bs(urlopen(next_url), 'html.parser')
        else:
            return new_list


def aggregate_hansard_corpus(doc_urls):
    transcripts = []

    corpusfilename = 'hansardcorpus.csv'
    recordfilename = 'hansardrecord.csv'

    record_has_header = False
    record_list = []
    waiting_for_reo_list = []

    with open(recordfilename, 'r') as record_file:
        record_has_header = csv.Sniffer().has_header(record_file)
        if record_has_header:
            rowcount = 0
            for row in csv.DictReader(record_file):
                record_list.append(row)
                rowcount + = 1
                if row['awaiting authorised reo'] is True:
                    waiting_for_reo_list.append(rowcount)

    if not record_has_header:
        with open(recordfilename, 'w') as record_file:
            head_writer = csv.writer(record_file)
            head_writer.writerow([
                'Date retreived',
                'Hansard document url',
                'wā',
                'title',
                'Te Reo length',
                'total length',
                'is Māori (%)',
                'awaiting authorised reo'
            ])

    corpus_has_header = False
    record_list = []

    with open(corpusfilename, 'r') as corpus:
        corpus_has_header = csv.Sniffer().has_header(corpus)
        if corpus_has_header:
            for row in csv.DictReader(record_file):
                record_list.append(row)

    if not corpus_has_header:
        with open(corpusfilename, 'w') as corpus:
            head_writer = csv.writer(corpus)
            head_writer.writerow([
                'Hansard document url',
                'wā',
                'title',
                'section number',
                'utterance number',
                'ingoa kaikōrero',
                'is Māori (%)',
                'kōrero waenga'
            ])

    last_record_url = record_list[-1]['Hansard document url']
    remaining_urls = doc_urls[doc_urls.index(last_record_url) + 1:]get_more_urls(last_record_url, doc_urls)

    with open(recordfilename, 'a') as record:
        with open(corpusfilename, 'a') as kiwaho:
            record_csv = csv.writer(record)
            hansard_csv = csv.writer(kiwaho)

            for doc_url in remaining_urls:
                corpus_writer(doc_url, record_csv, hansard_csv)

                print('---\n')


def corpus_writer(doc_url, record_csv, hansard_csv):
    transcripts, teReo_size, total_size, awaiting_teReo = HansardTuhingaScraper(
        doc_url).horoi_transcript_factory()

    record_csv.writerow([datetime.now()] + transcripts[0].listify[0:3] + [
                        teReo_size, total_size, teReo_size / total_size, awaiting_teReo])

    for transcript in transcripts:
        hansard_csv.writerow(transcript.listify())


def main():

    hansard_doc_urls = scrape_Hansard_URLs()

    aggregate_hansard_corpus(hansard_doc_urls)

    print('Corpus compilation successful\n')


if __name__ == '__main__':
    main()
