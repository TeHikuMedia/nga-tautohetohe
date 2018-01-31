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
index_filename = 'hathivolumeURLs.csv'
volumeindex_fieldnames = ['retreived', 'url', 'name',
                          'period', 'session', 'downloaded', 'processed']
dayindex_fieldnames = ['url', 'volume', 'date', 'reo', 'ambiguous', 'other',
                       'percent', 'retrieved', 'format', 'incomplete']
reo_fieldnames = ['url', 'volume', 'date', 'utterance', 'speaker', 'reo',
                  'ambiguous', 'other', 'percent', 'text']
# Processing the text is local resource intensive,
# therefore number of threads should be comparable to the CPU specs.
num_threads = 1
write_lock = Lock()


def get_file_list():
    volume_list = read_index_rows()
    file_list = [f for f in listdir(indir) if isfile(
        join(indir, f)) and f.endswith('.csv')]

    for v in volume_list:
        for f in file_list:
            if v['name'] == f[:f.index('.csv')] and not v['processed']:
                yield f


def read_index_rows():
    while True:
        rows = []
        with open(index_filename, 'r') as url_file:
            reader = csv.DictReader(url_file)
            for row in reader:
                rows.append(row)
            return rows


def process_csv_files():
    if not exists(outdir):
        makedirs(outdir)

    if not exists('hansardrāindex.csv'):
        with open('hansardrāindex.csv', 'w') as f:
            writer = csv.DictWriter(f, dayindex_fieldnames)
            writer.writeheader()
    if not exists('hansardreomāori.csv'):
        with open('hansardreomāori.csv', 'w') as f:
            writer = csv.DictWriter(f, reo_fieldnames)
            writer.writeheader()

    # with ThreadPool(num_threads) as pool:
    for name in map(process_csv, get_file_list()):
        i_rows = []
        r_rows = []
        with open('{}/{}rāindex.csv'.format(outdir, name), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                i_rows.append(row)
        with open('{}/{}reomāori.csv'.format(outdir, name), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                r_rows.append(row)
        with open('hansardrāindex.csv', 'a') as f:
            writer = csv.DictWriter(f, dayindex_fieldnames)
            writer.writerows(i_rows)
        with open('hansardreomāori.csv', 'a') as f:
            writer = csv.DictWriter(f, reo_fieldnames)
            writer.writerows(r_rows)


def process_csv(f):
    print('Extracting corpus from {}:'.format(f))

    volume = Volume(f)
    volume.process_pages()
    name = f[:f.index('.csv')]

    # Update the record of processed volumes:
    with write_lock:
        completion = 1
        rows = []
        for row in read_index_rows():
            if name == row['name']:
                row['processed'] = True
            rows.append(row)

        while True:
            with open(index_filename, 'w') as url_file:
                writer = csv.DictWriter(url_file, volumeindex_fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            break
    return name


class Volume(object):
    """docstring for Volume."""

    def __init__(self, filename):
        self.filename = filename
        self.day = {'format': 'OCR'}
        self.totals = {}
        self.row = {'utterance': 0}
        self.day['volume'] = self.row['volume'] = filename[:filename.index(
            '.csv')]
        self.flag294 = int(self.row['volume'].isdigit()
                           and int(self.row['volume']) >= 294)
        self.flag410 = int(self.flag294 and int(self.row['volume']) >= 410)

    def process_pages(self):
        # Invoke this method from a class instance to process the debates.
        with open('{}/{}{}'.format(outdir, self.row['volume'], 'rāindex.csv'), 'w') as output:
            writer = csv.DictWriter(output, dayindex_fieldnames)
            writer.writeheader()
        with open('{}/{}{}'.format(outdir, self.row['volume'], 'reomāori.csv'), 'w') as output:
            writer = csv.DictWriter(output, reo_fieldnames)
            writer.writeheader()

        with open('{}/{}'.format(indir, self.filename), 'r') as kiroto:
            reader = csv.DictReader(kiroto)
            day = []
            for page in reader:
                if not (page['url'].endswith(('c', 'l', 'x', 'v', 'i')) or page['page'] == '1') and re.search('[a-zA-Z]', page['text']):
                    day = self.__process_page(page, day)

    def __process_page(self, page, day):
        text = page['text']
        looped = 0

        while True:
            nextday = date_pattern[self.flag294].search(text)
            if nextday:
                previoustext = header_pattern.sub(
                    '', text[:nextday.start()], 1)
                day.append(previoustext)
                self.__process_day(day)

                day = []
                self.row['date'] = self.day['date'] = clean_whitespace(
                    nextday.group(0))
                self.row['url'] = self.day['url'] = page['url']
                self.day['retrieved'] = page['retreived']
                self.row['utterance'] = 0
                text = text[nextday.end():]
                looped += 1
            else:
                if looped:
                    day.append(text)
                else:
                    day.append(header_pattern.sub('', text, 1))
                break
        return day

    def __process_day(self, day):
        text = ''.join(day)

        # Remove hyphenated line breaks
        text = re.sub('(?<=[a-z]) *-\n+ *(?=[a-z])', '', text)
        # Remove name lists, ayes and noes
        # Remove lines with no letters, short lines, single word lines and lines of punctuation, capitals, digits
        regex = ['([A-Z][ a-zA-Z.]+, ){2}[A-Z][ a-zA-Z.]+\.', '(AYE|Aye|NOE|Noe)[^\n]*',
                 '[^A-Za-z]*', '[^\n]{1,2}', '[ \-\d,A-Z.?!:]+', '[a-zA-Z]+', ]
        for r in regex:
            text = re.sub('(?<=\n){}\n'.format(r), '', text)

        # Reset totals then process text:
        self.totals = {'reo': 0, 'ambiguous': 0, 'other': 0}
        self.__process_paragraphs(text)

        # Write day statistics
        if sum(self.totals.values()) > 50:
            self.day['percent'] = get_percentage(**self.totals)
            self.day.update(self.totals)
            with open('{}/{}{}'.format(outdir, self.row['volume'], 'rāindex.csv'), 'a') as output:
                writer = csv.DictWriter(output, dayindex_fieldnames)
                writer.writerow(self.day)

    def __process_paragraphs(self, text):
        utterance = []
        while True:
            p_break = paragraph_pattern.search(text)
            if p_break:
                utterance = self.__process_paragraph(
                    text[:p_break.start()], utterance)

                text = text[p_break.end():]
            else:
                utterance = self.__process_paragraph(text, utterance)
                if utterance:
                    self.__write_row(utterance)
                break

    def __process_paragraph(self, text, utterance):
        kaikōrero = newspeaker_pattern[self.flag410].match(text)
        if kaikōrero:
            name = ''
            if self.flag410:
                speaker = newspeaker_pattern[2].match(kaikōrero.group(3))
                if speaker:
                    name = speaker.group(2)
            else:
                speaker = kaikōrero.group(0)
                index1 = re.search('[A-Z]', speaker)
                if index1:
                    index2 = None
                    if speaker.endswith('—'):
                        index2 = re.search('(:|(. ?))?—', speaker).start()
                    elif speaker.endswith(':'):
                        index2 = speaker.index(':')
                    name = clean_whitespace(speaker[index1.start():index2])

            if name:
                if utterance:
                    self.__write_row(utterance)
                    utterance = []
                self.row['speaker'] = name
                text = text[kaikōrero.end():]

        return self.__process_sentences(text, utterance)

    def __process_sentences(self, text, utterance):
        consecutive = {'reo': True} if utterance else {'reo': False}
        consecutive['other'] = False
        loopflag, nums = True, {}

        while loopflag:
            nextsentence = new_sentence.search(text)
            if nextsentence:
                sentence = text[:nextsentence.start() + 1]
                text = text[nextsentence.end():]
            else:
                sentence = text
                loopflag = False

            c, nums = kupu_ratios(sentence)
            if c:
                sentence = clean_whitespace(sentence)
                if consecutive['reo']:
                    utterance.append(sentence)
                else:
                    consecutive['reo'] = True
                    consecutive['other'] = False
                    self.row['utterance'] += 1
                    utterance = [sentence]

            else:
                if not consecutive['other']:
                    if utterance:
                        self.__write_row(utterance)
                    utterance = []
                    consecutive['other'] = True
                    consecutive['reo'] = False
                    self.row['utterance'] += 1

                for k, v in nums.items():
                    if k != 'percent':
                        self.totals[k] += v

            if not loopflag:
                return utterance

    def __write_row(self, text):
        text = ' '.join(text)
        text = text[re.search('[a-zA-Z]', text).start():]
        bad_egg = re.match(
            '[A-Z][^ ]*(([^a-zA-Z]+[^ A-Z]*){1,2}[A-Z][^ ]*)*(([^a-zA-Z]+[^ A-Z]*){2})?', text)

        if not (bad_egg and len(bad_egg.group(0)) == len(text)):
            c, nums = kupu_ratios(text)
            for k, v in nums.items():
                if k != 'percent':
                    self.totals[k] += v

            if c and not (nums['reo'] < 5 and nums['other'] + nums['ambiguous'] < 10):
                self.row['text'] = text
                self.row.update(nums)
                print(self.row['text'])
                with open('{}/{}{}'.format(outdir, self.row['volume'], 'reomāori.csv'), 'a') as output:
                    writer = csv.DictWriter(output, reo_fieldnames)
                    writer.writerow(self.row)


# New header pattern from volume 359 onwards (5 Dec 1968), 440 onwards - first 3 lines, 466 onward - 1 line
header_pattern = re.compile(
    '[^\n]*\n((([^\n\]]*\n){0,5}[^\n]*\][^\n]*)\n)?((([^ \n]+( [^ \n,—]+){0,3}))\n)*(([^a-z][^\n:—]*( [^a-z\n][^ —:\n]*){2}[^-\n:]\n)+)*')
# best catch-all header pattern so far:
# ,"[^\n]*\n((([^\n]*\n){0,5}[^\n]*\][^\n]*)\n)?((([^ \n]+( [^ \n,—]+){0,3}))\n)*(([^a-z][^\n:—]*( [^a-z\n][^ —:\n]*){2}[^-\n:]\n)+)*


# Regex to look for meeting date. Date pattern changes from vol 294 onwards
date_pattern = [re.compile(
    r'\n[A-Z][a-z]{5,8}, [\dinISl&^]{1,2}[a-zA-Z]{2} [A-Z][!1Ia-z]{2,8}, [\d(A-Z]{4,5}'),
    re.compile(r'[A-Z][A-Za-z]{5,8}, \d{1,2} [A-Za-z]{3,9},? \d{4}[^\n–:!?]{0,4}\n')]

# Speaker pattern changes at volume 410 (19 May 1977). Pre-410 many passages are written
# as a narrative, so will process it as whole paragraphs.
newspeaker_pattern = [re.compile(
    '([A-Z .:—-]*\n)*[A-Z]([^(\n]+\([^-—\n]+[-—]*\n)?[a-zA-Z". ()]+\. ?[-—]+(?!\n)'),
    re.compile(
    '(([-~‘’\'() a-zA-Z]*\n)*)([^:\n]*:|([^,\n]*\n)[^:\n]*:)'),
    re.compile(
    '((\d{d}\.|The) )?(((Rt\.?|Right) )?(Hon\. )?(Mr\. )?([A-Z]([a-z{a}]+|[A-Z{a}]+|\.?))([ -{a}][tA-Z]([öa-z{a}]+|[ÖA-Z{a}]+|\.?))+)([-—]+| \(|:)'.format(a=apostrophes, d='{1,2}'))]
# name_behaviour = '((\d{1,2}\.|The) )?(((Rt\.?|Right) )?(Hon\. )?(Mr\. )?([A-Z]([a-z‘’\']+|[A-Z‘’\']+|\.?))([ -‘’\'][tA-Z]([öa-z‘’\']+|[ÖA-Z‘’\']+|\.?))+)([-—]+| \(|:)'

# Regex for splittting paragraphs
paragraph_pattern = re.compile(
    '(?<=([.!?]|[-—]))[-—.!? ‘’\'"•]*\n["\']*(?=[A-Z])')


def main():
    try:
        process_csv_files()
        print('Corpus aggregation successful\n')
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
