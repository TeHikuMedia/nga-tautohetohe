# import libraries
import csv
from pathlib import Path
import time
from datetime import datetime
import re
from taumahi import *
from os import listdir
from sys import getrecursionlimit, setrecursionlimit
from os.path import isfile, join

apostrophes = '‘’\''
sentence_end = ['[.!?]', '[{}]*'.format(apostrophes)]

# Regex to split paragraph into sentences
new_sentence = re.compile('{}{} '.format(sentence_end[0], sentence_end[1]))


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

# Regex to replace all tilda_vowels with macron vowels
vowel_map = {'a': 'ā', 'e': 'ē', 'i': 'ī', 'o': 'ō', 'u': 'ū'}
vowels = re.compile(r'(A?~|\[A macron\])([aeiouAEIOU])')


def process_txt_files(dirpath):
    corpusfilename = 'hansardcorpusPDF.csv'
    indexfilename = 'hansardindexPDF.csv'

    with open(indexfilename, 'w') as index:
        index_csv = csv.writer(index)
        index_csv.writerow([
            'Hansard volume',
            'Rā',
            'Num paragraphs',
            'Te Reo length',
            'Ambiguous length',
            'Other length',
            'is Māori (%)',
        ])
        with open(corpusfilename, 'w') as kiwaho:
            corpus_csv = csv.writer(kiwaho)
            corpus_csv.writerow([
                'Hansard volume',
                'Rā',
                'Ingoa kaikōrero',
                'Speaker turn',
                'Paragraph number',
                'Te Reo length',
                'Ambiguous length',
                'Other length',
                'Is Māori (%)',
                'Kōrero'
            ])

            for f in get_file_list(dirpath):
                print('\nProcessing {}:\n'.format(f))
                with open('{}/{}'.format(dirpath, f), 'r') as hansard_txt:
                    txt = vowels.sub(
                        tilda2tohutō, page_break.sub('\n', hansard_txt.read()))
                    txt = re.sub(r'\[[^\]]*]', '', txt)
                    tuhituhikifile(f, get_daily_debates(
                        txt), index_csv, corpus_csv)
                print('{} processed at {} after {}\n'.format(
                    f, datetime.now(), get_rate()))


def get_file_list(dirpath):
    files = [f for f in listdir(dirpath) if isfile(
        join(dirpath, f)) and f.endswith('.txt')]
    files.sort()
    return files


def tilda2tohutō(matchchar):
    return vowel_map[matchchar.group(2).lower()]


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
    if most_loops > loops:
        global longest_day
        longest_day = date.group(0)
    print('Processed {}'.format(date.group(0)))
    return debate_list


# Regex for splittting paragraphs
new_paragraph = re.compile(
    '({}+|-+){}\n'.format(sentence_end[0], sentence_end[1]))

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
        name = ''
        if kaikōrero:
            name_string = kaikōrero.group(3)
            if re.match('Vote|Ayes|Noes', name_string):
                line = re.match(r'[^\n]*\n', txt)
                txt = txt[line.end():]
                continue
            name = re.match(name_behaviour, name_string)
            if name:
                speeches.append(Speech(speaker, paragraphs))
                paragraphs = []
                speaker = name.group(2)
                txt = txt[kaikōrero.end():]

        paragraph_end = new_paragraph.search(txt)
        if paragraph_end:
            paragraphs.append(
                Paragraph(clean_whitespace(txt[:paragraph_end.start() + 1])))
            txt = txt[paragraph_end.end():]
        else:
            paragraphs.append(Paragraph(clean_whitespace(txt)))
            speeches.append(Speech(speaker, paragraphs))
            break

    global most_loops
    if loops > most_loops:
        most_loops = loops
    return speeches


def clean_whitespace(paragraph):
    return re.sub(r'\s+', ' ', paragraph).strip()


def tuhituhikifile(volume, debates, index_csv, corpus_csv):
    volume_totals = [0, 0, 0]
    for date, speeches in reversed(debates):
        totals = [0, 0, 0]

        turn = 0
        p_sum = 0
        for speech in speeches:
            turn += 1
            kaikōrero = speech.kaikōrero
            p_count = 0
            for paragraph in speech.paragraphs:
                p_count += 1
                if paragraph.condition:
                    print('{}: {}\nSpeaker {}: {}, paragraph {},\nMaori = {}%\n{}\n'.format(
                        volume, date, turn, kaikōrero, p_count, paragraph.ratios[3], paragraph.txt))
                    corpus_csv.writerow(
                        [volume, date, kaikōrero, turn, p_count] + paragraph.ratios + [paragraph.txt])
                for i in range(len(totals)):
                    totals[i] += paragraph.ratios[i]
            p_sum += p_count
        index_csv.writerow([volume, date, p_sum] + totals +
                           [get_percentage(totals[0], totals[1], totals[2])])
        for i in range(len(totals)):
            volume_totals[i] += totals[i]
    print(
        'Maori = {a}, Ambiguous = {b}, Non-Māori = {c}, Percentage = {d} %'.format(
            a=volume_totals[0], b=volume_totals[1], c=volume_totals[2], d=get_percentage(volume_totals[0], volume_totals[1], volume_totals[2])))


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
