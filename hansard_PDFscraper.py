# import libraries
import csv
from pathlib import Path
import time
from datetime import datetime
import re
from taumahi import kupu_ratios
from os import listdir
from sys import getrecursionlimit, setrecursionlimit
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

# Regex to check each paragraph matches for a new speaker, extract and replace with empty string
speaker_pattern = '{t}({s}|([^,\n]*\n){s}):'.format(t=titles, s=speaker)
new_speaker = re.compile(pattern=speaker_pattern)

# Regec to split paragraph into sentences
sentence_signal = '{}{} '.format(sentence_end[0], sentence_end[1])
new_sentence = re.compile(pattern=sentence_signal)


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


def process_txt_files(dirpath):
    corpusfilename = 'hansardcorpusPDF.csv'
    indexfilename = 'hansardindexPDF.csv'

    with open(indexfilename, 'w') as index:
        index_csv = csv.writer(index)
        index_csv.writerow([
            'Hansard volume',
            'Rā',
            'Te Reo length',
            'Ambiguous length'
            'Other length',
            'is Māori (%)',
        ])
        with open(corpusfilename, 'w') as kiwaho:
            corpus_csv = csv.writer(kiwaho)
            corpus_csv.writerow([
                'Hansard volume',
                'Rā',
                'Speaker turn',
                'Ingoa kaikōrero',
                'Paragraph number',
                'Te Reo length',
                'Ambiguous length'
                'Other length',
                'Is Māori (%)',
                'Kōrero'
            ])

            for f in get_file_list(dirpath):
                print('\nProcessing {}:\n'.format(f))
                with open('{}/{}'.format(dirpath, f), 'r') as hansard_txt:
                    txt = tilda_vowels.sub(
                        tilda2tohutō, page_break.sub('\n', hansard_txt.read()))
                    tuhituhikifile(f, get_daily_debates(
                        txt), index_csv, corpus_csv)
                print('{} processed\n'.format(f))


def get_file_list(dirpath):
    files = [f for f in listdir(dirpath) if isfile(
        join(dirpath, f)) and f.endswith('.txt')]
    files.sort()
    return files


def tilda2tohutō(machchar):
    return vowel_map[matchchar.group[0]]


def get_daily_debates(txt, date=None):
    if not date:
        date = debate_date.search(txt)
        txt = txt[date.end():]

    print('Processing {}'.format(date.group(0)))
    debate_map = {}
    nextdate = debate_date.search(txt)
    if nextdate:
        debate_map = get_daily_debates(txt=txt[nextdate.end():], date=nextdate)
        txt = txt[:nextdate.start()]
    debate_map[date.group(0)] = get_speeches(txt)[0]
    print('Processed {}'.format(date.group(0)))
    return debate_map


def get_speeches(txt):
    kaikōrero = new_speaker.match(txt)
    paragraph_end = new_paragraph.search(txt)
    paragraph = txt
    remaining_txt = None

    if paragraph_end:
        paragraph = txt[:paragraph_end.start() + 1]
        remaining_txt = txt[paragraph_end.end():]

    speeches = []
    paragraphs = []

    if remaining_txt:
        speeches, paragraphs = get_speeches(remaining_txt)

    if kaikōrero:
        # print('\ngroup0', kaikōrero.group(0))
        # print('group1', kaikōrero.group(1))
        # print('group2', kaikōrero.group(2))
        # print('group3', kaikōrero.group(3)) we want group 3
        # print('group4', kaikōrero.group(4))
        paragraph = paragraph[kaikōrero.end():]
        return [Speech(kaikōrero.group(
            3), [Paragraph(paragraph)] + paragraphs)] + speeches, []

    return speeches, [Paragraph(paragraph)] + paragraphs


def tuhituhikifile(volume, debates, index_csv, corpus_csv):
    for date, speeches in debates.items():
        totals = [0, 0, 0]

        turn = 0
        for speech in speeches:
            turn = turn + 1
            kaikōrero = speech.kaikōrero
            p_count = 0
            for paragraph in speech.paragraphs:
                p_count = p_count + 1
                if paragraph.condition:
                    print('{}: {}\nSpeaker {}: {}, paragraph {},\nMaori = {}%\n{}\n'.format(
                        volume, date, turn, kaikōrero, p_count, paragraph.ratios[3], paragraph.txt))
                    corpus_csv.writerow(
                        [volume, date, turn, kaikōrero, p_count] + paragraph.ratios + [paragraph.txt])
                for i in range(len(totals)):
                    totals[i] = totals[i] + paragraph.ratios[i]
        index_csv.writerow([volume, date] + totals)


def main():

    start_time = time.time()

    setrecursionlimit(4000)

    process_txt_files(dirpath='1987-2002')

    print('Corpus compilation successful\n')
    print("\n--- Job took %s seconds ---\n" % (time.time() - start_time))


if __name__ == '__main__':
    main()
