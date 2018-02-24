# import libraries
import csv
import re
import time
from pathlib import Path
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs
from datetime import datetime
from taumahi import *

hansard_url = 'https://www.parliament.nz'
hansard_meta_url = f'{hansard_url}{"/en/document/"}'
htmlindexfilename = 'hansardhtmlindex.csv'
rāindexfilename = 'hansardrāindex.csv'
corpusfilename = 'hansardreomāori.csv'
rāindex_fieldnames = ['retrieved', 'url', 'volume', 'format', 'date1', 'date2', 'reo', 'ambiguous', 'other', 'percent',
                      'incomplete']
reo_fieldnames = ['url', 'volume', 'format', 'date1', 'date2', 'utterance', 'speaker', 'reo', 'ambiguous', 'other',
                  'percent', 'text']

# Vars for generating clean numeric dates from OCRed dates:
months = {1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun', 7: 'jul', 8: 'aug',
          9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec'}
inv_months = {v: k for k, v in months.items()}
rā = māhina = tau = None


class HansardTuhingaScraper:
    """Class for scraping HTML formatted debates from websites."""

    def __init__(self, url):
        self.doc_id = url.split('/')[6]
        self.url = f'{hansard_url}{url}'
        self.soup = self.metasoup = self.kōrero_hupo = None
        self.hanga_hupo()
        self.retrieved = datetime.now()

    def hanga_hupo(self):
        # query the website and parse the returned html using beautiful soup
        alternative_url = f'{hansard_meta_url}{self.doc_id}'
        get_stuff = ''
        exception_flag = None
        count = 0
        while True:
            try:
                get_stuff = urlopen(self.url)
                break
            except Exception as e:
                count += 1
                if count > 3:
                    print(e, '\nTrying alternative URL...')
                    try:
                        get_stuff = urlopen(alternative_url)
                        exception_flag, self.url = True, alternative_url
                        print('\nSuccess!\n')
                        break
                    except Exception as e:
                        raise Exception(e, '\nCould not find data')
                else:
                    time.sleep(3)

        self.soup = bs(get_stuff, 'html.parser')

        if exception_flag:
            self.kōrero_hupo = self.soup.find('div', attrs={'class': 'section'}).select('div.section > div.section')
        elif re.match(r'\d', self.doc_id):
            self.kōrero_hupo = self.soup.select('div.Hansard > div')
        else:
            self.kōrero_hupo = self.soup.find_all('div', attrs={'class': 'section'})

        # Make soup from hansard metadata
        meta_url = f'{alternative_url}{"/metadata"}'
        while True:
            try:
                self.metasoup = bs(urlopen(meta_url), 'html.parser').table
                break
            except:
                time.sleep(3)

    def horoi_transcript_factory(self):
        meta_entries = {}
        meta_data = self.metasoup.find_all('tr')
        for tr in meta_data:
            meta_entries[tr.th.get_text(" ", strip=True).lower()] = tr.td.get_text(" ", strip=True)
        i_row = {'url': self.url, 'format': 'HTML',
                 'volume': re.search(r'Volume\s*([0-9]{3})', meta_entries['ref']).group(1),
                 'date1': meta_entries['date']}
        match = re.match('(\d{1,2}).([a-zA-Z]{3}).(\d{4})', meta_entries['date'])
        i_row['date2'] = f'{match.group(3)}-{inv_months[match.group(2).lower()]}-{match.group(1)}'
        c_rows, c_row = [], {'utterance': 0}
        totals = {'reo': 0, 'ambiguous': 0, 'other': 0}
        for k, v in i_row.items():
            c_row[k] = v
        i_row.update({'retrieved': self.retrieved, 'incomplete': ''})

        print('\n{}\n'.format(self.url))

        for section in self.kōrero_hupo:
            p_list = section.find_all('p')
            print('Paragraphs =', len(p_list))
            for paragraph in p_list:
                flag = check = False

                # Check for a new speaker:
                strong_tags = paragraph.find_all('strong')
                for strong in strong_tags:
                    string = strong.get_text(" ")
                    if not flag and string and re.search(r'[a-zA-Z]{5,}', string):
                        flag, c_row['speaker'] = True, clean_whitespace(string)
                    strong.replace_with(' ')

                kōrero = paragraph.get_text(" ", strip=True)
                if re.match(r'\[.*\]', kōrero):
                    if re.match(r'\[Authorised Te Reo text', kōrero):
                        i_row['incomplete'] = check = True
                    else:
                        continue

                if flag:
                    p = kōrero.split(':', 1)[-1].strip()
                    if p:
                        kōrero = p

                if re.search(r'[a-zA-Z]', kōrero):
                    c_row['utterance'] += 1

                    save_corpus, nums = kupu_ratios(kōrero)

                    for k, v in nums.items():
                        if k != 'percent':
                            totals[k] += v

                    if (save_corpus and nums['reo'] > 2) or check:
                        c_row.update(nums)
                        c_row['text'] = clean_whitespace(kōrero)
                        print(
                            '{date1}: {title}\nutterance {utterance}, Maori = {reo}%\nname:{speaker}\n{text}\n'.format(
                                title=meta_entries['short title'], **c_row))
                        c_rows.append(dict(c_row))
        print('Time:', self.retrieved)
        i_row['percent'] = get_percentage(**totals)
        i_row.update(totals)
        return c_rows, i_row


def scrape_hansard_urls():
    doc_url_list = []

    if Path(htmlindexfilename).exists():
        with open(htmlindexfilename, 'r', newline='', encoding='utf8') as url_file:
            for row in csv.DictReader(url_file):
                doc_url_list.append(row['url'])
    else:
        with open(htmlindexfilename, 'w', newline='', encoding='utf8') as url_file:
            csv.writer(url_file).writerow(['retreived', 'url'])

    last_url = ''
    if doc_url_list:
        last_url = doc_url_list[-1]
    new_list = get_new_urls(last_url)

    with open(htmlindexfilename, 'a', newline='', encoding='utf8') as url_file:
        url_writer = csv.writer(url_file)
        for url in reversed(new_list):
            doc_url_list.append(url[1])
            url_writer.writerow(url)

    print('\nCollected all URLs\n')

    return doc_url_list


def get_new_urls(last_url):
    rhr_soup = bs(urlopen(f'{hansard_url}{"/en/pb/hansard-debates/rhr/"}'), 'html.parser')

    new_list = []
    while True:
        print('\nChecking for new kōrerorero Hansard\n')

        retreivedtime = datetime.now()
        for h2 in rhr_soup.select('ul.hansard__list h2'):
            new_url = h2.a['href']
            if new_url == last_url:
                return new_list
            else:
                print(new_url)
                new_list.append([retreivedtime, new_url])

        next_page = rhr_soup.find('li', attrs={'class', 'pagination__next'})

        if next_page:
            next_url = f'{hansard_url}{next_page.find("a")["href"]}'
            rhr_soup = bs(urlopen(next_url), 'html.parser')
        else:
            return new_list


def aggregate_hansard_corpus(doc_urls):
    record_list = []
    if Path(rāindexfilename).exists():
        with open(rāindexfilename, 'r', newline='', encoding='utf8') as i:
            record_list = [row for row in csv.DictReader(i)]

            # rowcount = 0
            # Doesn't do anything yet:
            # waiting_for_reo = [row if row['incomplete'] for row in record_list]
            # for row in record_list:
            #     if row['incomplete']:
            #         waiting_for_reo.append(rowcount)
            #     rowcount += 1
    else:
        with open(rāindexfilename, 'w', newline='', encoding='utf8') as i:
            csv.DictWriter(i, rāindex_fieldnames).writeheader()

    if not Path(corpusfilename).exists():
        with open(corpusfilename, 'w', newline='', encoding='utf8') as c:
            csv.DictWriter(c, reo_fieldnames).writeheader()

    remaining_urls = []
    if record_list:
        last_record_url = record_list[-1]['url'].replace(hansard_url, '')
        remaining_urls = doc_urls if (last_record_url not in doc_urls) else doc_urls[
                                                                            doc_urls.index(last_record_url) + 1:]
    else:
        remaining_urls = doc_urls

    for doc_url in remaining_urls:
        c_rows, i_row = HansardTuhingaScraper(doc_url).horoi_transcript_factory()

        with open(rāindexfilename, 'a', newline='', encoding='utf8') as i, open(corpusfilename, 'a', newline='',
                                                                                encoding='utf8') as c:
            csv.DictWriter(i, rāindex_fieldnames).writerow(i_row)
            if c_rows:
                csv.DictWriter(c, reo_fieldnames).writerows(c_rows)

        print('---\n')


def main():
    start_time = time.time()

    hansard_doc_urls = scrape_hansard_urls()
    aggregate_hansard_corpus(hansard_doc_urls)

    print('Web Hansard scraping successful')
    print(f"--- Job took {time.time() - start_time} seconds ---\n")


if __name__ == '__main__':
    main()
