# import libraries
import csv
from pathlib import Path
import time
from datetime import datetime
import re
from taumahi import *
from os import listdir
from os.path import isfile, join

dayindex_fieldnames = ['url', 'volume', 'date', 'reo', 'ambiguous', 'other',
                       'percent', 'retrieved', 'format', 'incomplete']
reo_fieldnames = ['url', 'volume', 'date', 'utterance', 'speaker', 'reo',
                  'ambiguous', 'other', 'percent', 'text']


class Speech:
    """docstring for Speech."""

    def __init__(self, kaikōrero, paragraphs):
        self.kaikōrero = kaikōrero
        self.paragraphs = paragraphs


class Paragraph:
    """docstring for Speech."""

    def __init__(self, txt):
        self.txt = txt
        self.condition, self.ratios = kupu_ratios(txt)


# Regex to replace page breaks with new line
page_break = re.compile('(\n{0,2}\d{1,2} [a-zA-Z]{3,9} \d{4}.*\n\n\f)')


def process_txt_files(dirpath):
    corpusfilename = 'hansardreomāori.csv'
    indexfilename = 'hansardrāindex.csv'

    if not exists('hansardrāindex.csv'):
        with open('hansardrāindex.csv', 'w') as f:
            writer = csv.DictWriter(f, dayindex_fieldnames)
            writer.writeheader()
    if not exists('hansardreomāori.csv'):
        with open('hansardreomāori.csv', 'w') as f:
            writer = csv.DictWriter(f, reo_fieldnames)
            writer.writeheader()

    with open('hansardrāindex.csv', 'a') as i, open('hansardreomāori.csv', 'a') as c:
        index_writer = csv.DictWriter(i, dayindex_fieldnames)
        corpus_writer = csv.DictWriter(c, reo_fieldnames)
        index_writer.writerows(i_rows)
        corpus_writer.writerows(r_rows)
        for f in get_file_list(dirpath):
            print('\nProcessing {}:\n'.format(f))
            with open('{}/{}'.format(dirpath, f), 'r') as hansard_txt:
                txt = sub_vowels(page_break.sub('\n', hansard_txt.read()))
                txt = re.sub(r'\[[^\]]*]', '', txt)
                tuhituhikifile(f, get_daily_debates(
                    txt), index_writer, corpus_writer)
            print('{} processed at {} after {}\n'.format(
                f, datetime.now(), get_rate()))


def get_file_list(dirpath):
    files = [f for f in listdir(dirpath) if isfile(
        join(dirpath, f)) and f.endswith('.txt')]
    files.sort()
    return files


# Regex to look for meeting date then split into date-debate key-value map
debate_date = re.compile(pattern=r'[A-Z]{6,9}, \d{1,2} [A-Z]{3,9} \d{4}')


def get_daily_debates(txt, date=None):
    if not date:
        date = debate_date.search(txt)
        txt = txt[date.end():]

    print('Processing {}'.format(date.group(0)))
    debate_list = []
    nextdate = debate_date.search(txt)
    if nextdate:
        debate_list = get_daily_debates(
            txt=txt[nextdate.end():], date=nextdate)
        txt = txt[:nextdate.start()]
    loops = most_loops
    debate_list.append([date.group(0), get_speeches(txt)])
    print('Processed {}'.format(date.group(0)))
    if most_loops > loops:
        global longest_day
        longest_day = date.group(0)
        print('Most strings! {}\n'.format(most_loops))
    return debate_list


# Regex to check each paragraph matches for a new speaker, then extracts the name
new_speaker = re.compile('{titles}({speaker}|([^,\n]*\n){speaker})'.format(
    titles='(([-~{}() a-zA-Z]*\n)*)'.format(apostrophes), speaker='[^:\n]*:'))
name_behaviour = re.compile('(\d{d}\. )?((Rt\.? )?(Hon\. )?([A-Z]([a-z{a}]+|[A-Z{a}]+|\.?))([ -{a}][tA-Z]([öa-z{a}]+|[ÖA-Z{a}]+|\.?))+)( \(|:)'.format(
    a=apostrophes, d='{1,2}'))


def get_speeches(txt):
    speeches = []
    paragraphs = []
    speaker = ''

    loops = 0
    while True:
        loops += 1
        if loops >= 1000 and loops % 500 == 0:
            print('Loops exceeded', loops)

        kaikōrero = new_speaker.match(txt)
        if kaikōrero:
            name = re.match(name_behaviour, kaikōrero.group(3))
            if name:
                speeches.append(Speech(speaker, paragraphs))
                paragraphs = []
                speaker = name.group(2)
                txt = txt[kaikōrero.end():]

        p, txt = get_paragraph(txt)
        paragraphs.append(Paragraph(p))
        if not txt:
            speeches.append(Speech(speaker, paragraphs))
            break

    global most_loops
    if loops > most_loops:
        most_loops = loops

    return speeches


def tuhituhikifile(volume, debates, index_writer, corpus_writer):
    for date, speeches in reversed(debates):
        totals = {'reo': 0, 'ambiguous': 0, 'other': 0}
        i_row = c_row = {'volume': volume[volume.index(
            ' ') + 1:volume.index(':')], 'date': date}
        c_row['utterance'] = 0
        for speech in speeches:
            for paragraph in speech.paragraphs:
                row['utterance'] += 1
                if paragraph.condition:
                    c_row.update({'text': clean_whitespace(
                        paragraph.txt), 'speaker': speech.kaikōrero})
                    c_row.update(paragraph.ratios)
                    print(
                        'Volume {volume}: {date}\n{speaker}: {utterance},\nMaori = {percent}%\n{text}\n'.format(**c_row))
                    corpus_writer.writerow(c_row)

                for k, v in paragraph.ratios.items():
                    if k != 'percent':
                        totals[k] += v
        i_row.update({'percent': get_percentage(**totals), 'format': 'PDF'})
        i_row.update(totals)
        index_writer.writerow(i_row)
        print(
            'Maori = {reo}, Ambiguous = {ambiguous}, Non-Māori = {other}, Percentage = {percent} %'.format(**i_row))


def main():
    try:
        process_txt_files(dirpath='1987-2002')
        print('Corpus compilation successful\n')
    except Exception as e:
        print(e)
    finally:
        print("\n--- Job took {} ---".format(get_rate()))
        print('Looped through {} strings while processing {}\n'.format(
            most_loops, longest_day))


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
