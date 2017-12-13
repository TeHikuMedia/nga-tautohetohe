# import libraries
import csv
from urllib.request import urlopen
import re
from bs4 import BeautifulSoup as bs
from datetime import datetime

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
                    if not flag and string and re.search(r'[a-zA-Z]{4,}', string):
                        ingoa_kaikōrero = string.strip()
                        flag = True

                kōrero_waenga = paragraph.get_text(strip=True)

                if flag:
                    # p = re.search(r'(?<=:).*', kōrero_waenga)
                    p = kōrero_waenga.split(':', 1)[-1].strip()
                    if p:
                        kōrero_waenga = p

                # heMāori = quant(paragraph)
                heMāori = 51

                if re.search(r'[a-zA-Z]', kōrero_waenga):
                    paragraph_count += 1
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
        return transcripts


def scrape_Hansard_URLs():
    list_url = '{}{}'.format(
        hansard_url, '/en/pb/hansard-debates/rhr/')

    rhr_soup = bs(urlopen(list_url), 'html.parser')

    doc_url_list = []

    while True:
        for h2 in rhr_soup.select('ul.hansard__list h2'):
            doc_url = h2.a['href']
            print(doc_url)
            doc_url_list.append(doc_url)

        next_page = rhr_soup.find(
            'li', attrs={'class', 'pagination__next'})

        if next_page:
            next_url = '{}{}'.format(hansard_url, next_page.find(
                'a')['href'])
            rhr_soup = bs(urlopen(next_url), 'html.parser')
        else:
            break
    return doc_url_list


def aggregate_hansard_corpus(doc_urls):
    doc_urls = doc_urls
    transcripts = []

    filename = "hansardcorpus.csv"

    with open(filename, 'w') as kiwaho:
        hansard_csv = csv.writer(kiwaho)
        # write the header
        hansard_csv.writerow([
            'Hansard document url',
            'wā',
            'title',
            'section number',
            'utterance number',
            'ingoa kaikōrero',
            'is Māori (%)',
            'kōrero waenga'
        ])
        for doc_url in doc_urls:
            transcripts = HansardTuhingaScraper(
                doc_url).horoi_transcript_factory()
            for transcript in transcripts:
                hansard_csv.writerow(transcript.listify())

            print('---\n')

            transcripts.append(transcripts)


def main():

    hansard_doc_urls = scrape_Hansard_URLs()

    aggregate_hansard_corpus(hansard_doc_urls)

    print('Corpus compilation successful\n')


if __name__ == '__main__':
    main()
