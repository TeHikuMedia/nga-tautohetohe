# import libraries
import csv
from pathlib import Path
import time
from datetime import datetime
import re
from taumahi import *
from os import listdir, makedirs
from multiprocessing.dummy import Process, Pool as ThreadPool, Lock
from os.path import isfile, join, exists

indir = 'volumes'
outdir = 'processed'
# Processing the text is local resource intensive,
# therefore number of threads should be comparable to the CPU specs.
num_threads = 8


def get_file_list():
    files = [f for f in listdir(indir) if isfile(
        join(indir, f)) and f.endswith('.csv')]
    files.sort()
    return files


def process_csv_files():
    if not exists(outdir):
        makedirs(outdir)
    with ThreadPool(num_threads) as pool:
        results = pool.map(process_csv, get_file_list())


def process_csv(f):
    print('\nProcessing {}:\n'.format(f))

    volume = Volume(f)
    volume.process_pages()

    print('{} processed at {} after {}\n'.format(
        f, datetime.now(), get_rate()))


class Volume(object):
    """docstring for Volume."""
    reofieldnames = ['url', 'volume', 'date', 'utterance', 'speaker', 'reo',
                     'ambiguous', 'other', 'percent', 'text']
    dayfieldnames = ['url', 'volume', 'date', 'reo', 'ambiguous', 'other',
                     'percent', 'retrieved', 'format', 'incomplete']

    def __init__(self, filename):
        self.filename = filename
        row = {'volume': filename[:filename.index('.')]}
        self.flag359 = int(row['volume'].isdigit()
                           and int(row['volume']) >= 359)
        self.flag410 = int(self.flag359 and int(row['volume']) >= 410)
        self.row = row
        self.url = ''
        self.kaikōrero = ''
        self.totals = {'format': 'OCR', 'incomplete': None}

    def process_pages(self):
        # Invoke this method from a class instance to process the debates.
        with open('{}/{}{}'.format(outdir, self.row['volume'], 'rāindex.csv'), 'w') as output:
            writer = csv.DictWriter(output, self.dayfieldnames)
            writer.writeheader()
        with open('{}/{}{}'.format(outdir, self.row['volume'], 'reomāori.csv'), 'w') as output:
            writer = csv.DictWriter(output, self.reofieldnames)
            writer.writeheader()

        with open('{}/{}'.format(indir, self.filename), 'r') as kiroto:
            reader = csv.DictReader(kiroto)
            day = []
            self.row['utterance'] = 0
            for page in reader:
                if not (page['url'].endswith(('c', 'l', 'x', 'v', 'i')) or page['page'] == '1'):
                    day = self.__process_page(page, day)

    def __process_page(self, page, day):
        text = ''

        # Substitute in macrons on the off chance they are detected
        # text = vowels.sub(tilda2tohutō, page_break.sub('\n', hansard_txt.read()))

        # Remove page header: potentially use a flag self.flag359
        text = header_pattern[0].sub('', page['text'], 1)

        while True:
            nextday = date_pattern.search(text)
            if nextday:
                day.append(text[:nextday.start()])
                self.__process_day(day)

                self.row['date'] = nextday.group(0)
                self.row['url'] = page['url']
                self.day.update(self.row)
                self.day.retrieved = page['retreived']
                self.row['utterance'] = 0
                text = text[nextday.end():]
            else:
                day.append(text)
                return day

    def __process_day(self, day):
        text = ''.join(day)
        # Remove any written notes
        text = re.sub(r'\[[^\]]*]', '', text)

        # Reset totals then process text:
        self.totals = {'reo': 0, 'ambiguous': 0, 'other': 0}
        self.__split_string(text, self.__process_paragraph, paragraph_pattern)

        # Write day statistics
        self.day.update(self.totals)
        print(self.day['date'])
        with open('{}/{}{}'.format(outdir, self.row['volume'], 'rāindex.csv'), 'a') as output:
            writer = csv.DictWriter(output, self.dayfieldnames)
            writer.writerow(self.totals)

    def __split_string(self, text, process_function, regex):
        while True:
            nextstring = regex.search(text)
            if nextstring:
                process_function(text[:nextstring.start()])
                text = text[nextstring.end():]
            else:
                process_function(text)

    def __process_paragraph(self, text):
        self.row['utterance'] += 1
        kaikōrero = newspeaker_pattern[self.flag410].match(text)
        if kaikōrero:
            # name = re.match(name_behaviour, kaikōrero.group(3))
            # if name:
            self.kaikōrero = name.group(0)
            text = text[kaikōrero.end():]

        # split_string(text, process_sentence, sentence_pattern)
        # or:
        self.__process_string(text)

    def __process_string(self, text):
        condition, nums = kupu_ratios(text)
        for k, v in nums.items():
            if k != 'percent':
                self.totals[k] += v

        if condition:
            self.row.update({'text': clean_whitespace(text)})
            self.row.update(nums)
            with open('{}/{}{}'.format(outdir, self.row['volume'], 'reomāori.csv'), 'a') as output:
                writer = csv.DictWriter(output, self.reofieldnames)
                writer.writerow(row)


def tilda2tohutō(matchchar):
    return vowel_map[matchchar.group(2).lower()]


# New header pattern from volume 359 onwards (5 Dec 1968)
header_pattern = [re.compile(
    '(([^\n]+\n){0,5}[^\n]*\][^\n]*)\n(([\da-zA-Z]+|([^ \n]+( [^ \n,]+){0,2}))\n)*|([^a-z][^\n]*\n){0,5}'),
    '']

# Regex to look for meeting date
date_pattern = re.compile(
    r'\n[A-Z][a-z]{5,8}, [\diISl&^]{1,2}[a-zA-Z]{2} [A-Z][!1Ia-z]{2,8}, [\d(A-Z]{4,5}')

# Speaker pattern changes at volume 410 (19 May 1977). Pre-410 many passages are written
# as a narrative, so will process it as whole paragraphs.
newspeaker_pattern = [re.compile('([A-Z .:—-]*\n)*[A-Z]([^(\n]+\([^-—\n]+[-—]*\n)?[a-zA-Z". ()]+\. ?[-—]+(?!\n)'),
                      re.compile('(([-~‘’\'() a-zA-Z]*\n)*)([^:\n]*:|([^,\n]*\n)[^:\n]*:)')]
# name_behaviour = '((\d{1,2}\.|The) )?(((Rt\.?|Right) )?(Hon\. )?(Mr\. )?([A-Z]([a-z‘’\']+|[A-Z‘’\']+|\.?))([ -‘’\'][tA-Z]([öa-z‘’\']+|[ÖA-Z‘’\']+|\.?))+)([-—]+| \(|:)'
name_pattern = re.compile('(\d{d}\. )?(((Rt\.?|Right) )?(Hon\. )?([A-Z]([a-z{a}]+|[A-Z{a}]+|\.?))([ -{a}][tA-Z]([öa-z{a}]+|[ÖA-Z{a}]+|\.?))+)([-—]+| \(|:)'.format(
    a=apostrophes, d='{1,2}'))

# Regex for splittting paragraphs
paragraph_pattern = re.compile('(?<=([.!?]|[-—]))[-—.!? ‘’\'"•]*\n(?=[A-Z])')


def main():
    try:
        process_csv_files()
        print('Corpus compilation successful\n')
    except Exception as e:
        raise e
    finally:
        print("\n--- Job took {} ---".format(get_rate()))


start_time = time.time()


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
