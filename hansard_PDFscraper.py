# import libraries
import csv
from pathlib import Path
import time
from datetime import datetime
import re
from taumahi import kupu_ratios
from os import listdir
from os.path import isfile, join


titles = '(([-~\'`() a-zA-Z]*\n)*)'
speaker = '[^:\n]*'
sentence_end = ['[.!?]', '[\'`]*']
page_endings = '(\n?\d{1,2} [a-zA-Z]{3,9} \d{4}.*\n\n\f)'

# Regex to replace page breaks with new line
page_break = re.compile(pattern=page_endings)

# Regex to replace all tilda_vowels with macron vowels
vowel_map = {'~a': 'ā', '~e': 'ē', '~i': 'ī', '~o': 'ō', '~u': 'ū'}
tilda_vowels = re.compile('~a|~e|~i|~o|~u')

# Regex to look for meeting date then split into date-debate key-value map
debate_date = re.compile(pattern=r'[A-Z]{6,9}, \d{1,2} [A-Z]{3,9} \d{4}')

# Regex for splittting paragraphs
paragraph_signal = '({}+|-+){}\n'.format(sentence_end[0], sentence_end[1])
new_paragraph = re.compile(pattern=paragraph_signal)

# Regex to check each paragrah for a new speaker, extract and replace with empty string
speaker_pattern = '{t}({s}|([^,\n]*\n){s}):'.format(t=titles, s=speaker)
new_speaker = re.compile(pattern=speaker_pattern)

# Regec to split paragraph into sentences
sentence_signal = '{}{} '.format(sentence_end[0], sentence_end[1])
new_sentence = re.compile(pattern=sentence_signal)

# ([.!?]+|-+)[\'`]*\n([-~\'`() a-zA-Z]*\n)*(\n?\d{1,2} [a-zA-Z]{3,9} \d{4}.*\n\n\f)?([-~\'`() a-zA-Z]*\n)*([^:\n]*:|[^,\n]*\n[^:\n]*:)


def read_txt_files(dirpath):
    volumes = {}
    for f in listdir(dirpath) if isfile(join(dirpath, f)) and f.endswith('.txt'):
        with open(f, 'r') as hansard_txt:
            txt = tilda_vowels.(tilda2tohutō, page_break.sub('\n', hansard_txt.read()))
            volumes[f] = get_daily_debates(txt)

            vol = []
            page = []
            speeches = []
            paragraph = []
            colon_index = 0
            day = ''
            ingoa_kaikōrero = ''
            fullstop_flag = True

            for line in hansard_txt:
                string = line.strip()
                c, n = 0, 0

                if debate_date.match(string):
                    day = string
                    continue

                for char in re.findall('\w', line) if char:
                    if capitals.match(char):
                        c += 1
                    if fullstop_flag and char is ':':
                        colon_index = n
                        ingoa_kaikōrero = line[0:n]
                    n += 1
                if c == n:
                    continue

                for strong in strong_tags:
                    string = strong.extract().string
                    if not flag and string and re.search(r'[a-zA-Z]{5,}', string):
                        ingoa_kaikōrero = string.strip()
                        fullstop_flag = True

                stripped = line.strip()
                paragraph.append(line)
                if stripped.endswith('.'):
                    speeches.append(paragraph)
                    paragraph = []

                if line.startswith('\f'):
                    vol.append(page)
                    process(page)
                    page = []
                page.append(line)


def tilda2tohutō(char):
    return vowel_map[char]


def get_daily_debates(txt, date=None):
    if not date:
        date = debate_date.search(txt)
        txt = txt[date.end():]

    debate_map = {}
    nextdate = debate_date.search(txt)
    if nextdate:
        debate_map = get_daily_debates(txt=txt[nextdate.end():], date=nextdate)
        # can call a cleaning function on the method here to create clean txt sublists
        cleaned_collection = clean_txt(txt[:nextdate.start()])

    debate_map[date] = cleaned_collection
    return debate_map


def clean_txt(txt):
    speeches = get_speeches(txt)

    # Regex to check each paragrah for a new speeker, extract and replace with empty string
    speaker_pattern = '{t}({s}|[^,\n]*\n{s})'.format(t=titles, s=speaker)
    new_speaker = re.compile(pattern=speaker_pattern)

    # Regec to split paragraph into sentences
    sentence_signal = '{}{} '.format(sentence_end[0], sentence_end[1])
    new_sentence = re.compile(pattern=sentence_signal)


def get_speeches(txt):
    speech, remaining_txt = get_paragraphs(txt)

    if remaining_txt:
        speeches = get_speeches(remaining_txt)

    return speeches


def get_speeches(txt, kaikōrero=None):
    paragraph_end = new_paragraph.search(txt)
    remaining_txt = None

    if paragraph_end:
        txt = [txt[:paragraph_end.start() + 1]]
        remaining_txt = txt[paragraph_end.end():]

    speeches = []
    paragraphs = []

    if remaining_txt:
        speeches, paragraphs = get_speeches(remaining_txt)
        new_kaikōrero = new_speaker.match(remaining_txt)
        if new_kaikōrero:
            speeches.append(Speech(new_kaikōrero.group(2), paragraphs))
            paragraphs = []

    tereo_stats = rate(txt)

    return speeches, [txt] + paragraphs


def process(page):
    pattern = r''
    re.match(pattern=pattern, page[0])
    pass


class HansardTuhingaScraper:
    def __init__(self, txtfile):
        ''' Set up our tuhituhi CorpusCollector with basic params '''
        self.txtfile = txtfile
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
        teReo_size = 0
        ambiguous_size = 0
        total_size = 0
        awaiting_teReo = None
        section_count = 0

        print('\n{}\n'.format(doc_url))

        for section in self.kōrero_hupo:

            section_count += 1
            # print('section:', section_count)
            paragraph_count = 0

            ingoa_kaikōrero = ''

            p_list = section.find_all('p')
            print('Paragraphs =', len(p_list))

            for paragraph in p_list:

                # print('paragraph: ', paragraph_count)

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

                    save_corpus, numbers = kupu_ratios(kōrero_waenga)

                    teReo_size += numbers[0]
                    ambiguous_size += numbers[1]
                    other_size += numbers[2]

                    if not save_corpus:
                        awaiting_teReo = re.match(
                            r'\[Authorised Te Reo text', text)
                        if awaiting_teReo:
                            save_corpus = True

                    if save_corpus:
                        print('{}: {}\nsection {}, paragraph {}, Maori = {}%\nname:{}\n{}\n'.format(
                            wā, title, section_count,
                            paragraph_count, heMāori, ingoa_kaikōrero, kōrero_waenga))
                        transcripts.append([doc_url, wā, title, section_count, paragraph_count,
                                            ingoa_kaikōrero] + numbers + [kōrero_waenga])
        print('Time:', self.retreived)
        doc_record = [self.retreived, self.doc_url, wā, title, teReo_size, ambiguous_size,
                      other_size, teReo_size / other_size, awaiting_teReo]
        return transcripts, doc_record


def scrape_Hansard_URLs():
    filename = 'urlindex.csv'

    has_header = False
    txt_path_list = []

    if Path(filename).exists():
        with open(filename, 'r') as url_file:
            for row in csv.DictReader(url_file):
                doc_url_list.append(row['url'])
    else:
        with open(filename, 'w') as url_file:
            csv.writer(url_file).writerow(['Date retreived', 'url'])

    last_url = ''
    if doc_url_list:
        last_url = doc_url_list[-1]
    new_list = get_new_urls(last_url)

    with open(filename, 'a') as url_file:
        url_writer = csv.writer(url_file)
        for url in reversed(new_list):
            doc_url_list.append(url[1])
            url_writer.writerow(url)

    print('\nCollected all URLs\n')

    return doc_url_list


def get_file_list(dirpath):
    return [f for f in listdir(dirpath) if isfile(join(dirpath, f)) and f.endswith('.txt')]


def aggregate_hansard_corpus(doc_urls):
    transcripts = []

    corpusfilename = 'hansardcorpus.csv'
    recordfilename = 'hansardrecord.csv'

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
                'wā',
                'title',
                'Te Reo length',
                'Ambiguous length'
                'Other length',
                'is Māori (%)',
                'awaiting authorised reo'
            ])

        # corpus_list = []

    if not Path(corpusfilename).exists():
        #     with open(corpusfilename, 'r') as corpus:
        #         if corpus_has_header:
        #             for row in csv.DictReader(corpus):
        #                 record_list.append(row)
        # else:
        with open(corpusfilename, 'w') as corpus:
            head_writer = csv.writer(corpus)
            head_writer.writerow([
                'Hansard document url',
                'wā',
                'title',
                'section number',
                'utterance number',
                'ingoa kaikōrero',
                'Te Reo length',
                'Ambiguous length'
                'Other length',
                'is Māori (%)',
                'kōrero waenga'
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


class Speech:
    """docstring for Speech."""

    def __init__(self, kaikōrero, paragraphs):
        self.kaikōrero = kaikōrero
        self.paragraphs = paragraphs


class Paragraph:
    """docstring for Speech."""

    def __init__(self, txt, num_Māori, num_ambiguous, num_other):
        self.num_Māori = num_Māori
        self.num_ambiguous = num_ambiguous
        self.num_other = num_other
        self.percentage = num_Māori / (num_Māori + num_other)


def main():

    start_time = time.time()

    read_txt_files(dirpath='1987-2002')

    hansard_doc_urls = scrape_Hansard_URLs()

    aggregate_hansard_corpus(hansard_doc_urls)

    print('Corpus compilation successful\n')
    print("\n--- Job took %s seconds ---\n" % (time.time() - start_time))


if __name__ == '__main__':
    main()
