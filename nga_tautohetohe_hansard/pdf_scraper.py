# import libraries
import csv
import time
from datetime import datetime
from taumahi import *
from os import listdir
from os.path import isfile, join, exists
from bs4 import BeautifulSoup as bs

volumeindex_filename = 'hansardvolumeindex.csv'
rāindexfilename = 'hansardrāindex.csv'
corpusfilename = 'hansardreomāori.csv'
volumeindex_fieldnames = ['retrieved', 'url', 'name', 'period', 'session', 'format', 'downloaded', 'processed']
rāindex_fieldnames = ['retrieved', 'url', 'volume', 'format', 'date1', 'date2', 'reo', 'ambiguous', 'other', 'percent',
                      'incomplete']
reo_fieldnames = ['url', 'volume', 'format', 'date1', 'date2', 'utterance', 'speaker', 'reo', 'ambiguous', 'other',
                  'percent', 'text']

# Vars for generating numeric dates from literal regexed dates:
months = {1: 'january', 2: 'february', 3: 'march', 4: 'april', 5: 'may', 6: 'june', 7: 'july', 8: 'august',
          9: 'september', 10: 'october', 11: 'november', 12: 'december'}
inv_months = {v: k for k, v in months.items()}


class Speech:
    """This class stores the speaker name and the paragraphs of the speaker's speech."""

    def __init__(self, kaikōrero, paragraphs):
        self.kaikōrero = kaikōrero
        self.paragraphs = paragraphs


class Utterance:
    """This class stores a passage of homogeneous text as well is numerical information about the text."""

    def __init__(self, txt):
        self.txt = txt
        self.condition, self.ratios = kupu_ratios(txt)


def process_txt_files(dirpath):
    # Create output files if not exists:
    if not exists(rāindexfilename):
        with open(rāindexfilename, 'w', newline='', encoding='utf8') as f:
            csv.DictWriter(f, rāindex_fieldnames).writeheader()
    if not exists(corpusfilename):
        with open(corpusfilename, 'w', newline='', encoding='utf8') as f:
            csv.DictWriter(f, reo_fieldnames).writeheader()

    # Iterate through volume file list:
    for f, v in get_file_list(dirpath):
        print(f'\nProcessing {f}:\n')

        # Read from volume text files
        txt = None
        with open(f'{dirpath}/{f}', 'r', newline='', encoding='utf8') as hansard_txt:
            txt = sub_vowels(page_break.sub('\n', hansard_txt.read()))
            txt = re.sub(r'\[[^\]]*]', '', txt)

        # Sort through text with RegEx,
        # Extracting te reo corpus and information about each day of debates,
        # Write to file:
        with open(rāindexfilename, 'a', newline='', encoding='utf8') as i, open(corpusfilename, 'a', newline='',
                                                                                encoding='utf8') as c:
            tuhituhikifile(v, get_daily_debates(txt), csv.DictWriter(i, rāindex_fieldnames),
                           csv.DictWriter(c, reo_fieldnames))

        # Update record of processed volumes:
        v_rows = []
        with open(volumeindex_filename, 'r', newline='', encoding='utf8') as vol_file:
            reader = csv.DictReader(vol_file)
            for row in reader:
                if row['name'] == v['name']:
                    row['processed'] = True
                v_rows.append(row)
        with open(volumeindex_filename, 'w', newline='', encoding='utf8') as vol_file:
            writer = csv.DictWriter(vol_file, volumeindex_fieldnames)
            writer.writeheader()
            writer.writerows(v_rows)
        print(f'{f} processed at {datetime.now()} after {get_rate()}\n')


def get_file_list(dirpath):
    # Get list of volumes from the volume index file:
    volume_list = read_index_rows()

    # Get list of text files generated from pdf volumes:
    file_list = [f for f in listdir(dirpath) if isfile(join(dirpath, f)) and f.endswith('.txt')]
    file_list.sort()

    # Return list items if they haven't been processed yet:
    for f in file_list:
        name = f[f.index(' ') + 1:f.index('.txt')]
        for v in volume_list:
            if v['name'] == name:
                if not v['processed']:
                    yield f, v
                break


def scrape_volume_urls(last_index):
    index = 483 if not (last_index and last_index.isdigit()) else int(last_index) + 1
    switch2 = index > 605  # Break condition
    index += 6

    # Scrape meta data from table list of Hansard volumes
    for tr in bs(urlopen('https://www.parliament.nz/en/pb/hansard-debates/historical-hansard/'), 'html.parser').select(
            '.wikitable')[0]('tr')[index:]:
        # Sort data from each cell of each row of table list into list of dictionaries
        row = {'format': 'PDF', 'downloaded': True, 'processed': None}
        row_cells = tr('td')
        switch3 = False  # 0 : second column, 1 : third column
        for cell in row_cells:
            a = cell.find(lambda tag: tag.has_attr('href'))
            if a:
                name = cell.get_text(' ', strip=True)
                switch2 = int(name) > 605
                if switch2:
                    break
                else:
                    row['name'] = name
                    row['url'] = a['href']
                    row['retrieved'] = datetime.now()
            else:
                if switch3:  # third column
                    row['session'] = cell.get_text(' ').strip()
                else:  # second column
                    row['period'] = cell.get_text(' ').strip()
                    switch3 = True
        else:
            print('Got link to volume:', row['name'])
            yield row
        if switch2:
            break


def read_index_rows():
    while True:
        rows = []
        # Read the volume index file
        with open(volumeindex_filename, 'r', newline='', encoding='utf8') as v_index:
            reader = csv.DictReader(v_index)
            for row in reader:
                rows.append(row)
        last_entry = rows[-1]['name']

        # Scrape remaining volume urls from parliament website & save to file if the index doesn't have them yet:
        if not last_entry.isdigit() or int(last_entry) < 606:
            with open(volumeindex_filename, 'a', newline='', encoding='utf8') as v_index:
                writer = csv.DictWriter(v_index, volumeindex_fieldnames)
                for entry in scrape_volume_urls(last_entry):
                    writer.writerow(entry)
                    rows.append(entry)

        return rows


def get_daily_debates(txt, date=None):
    if not date:
        date = debate_date.search(txt)
        txt = txt[date.end():]

    print(f'Processing {date.group(0)}')
    debate_list = []
    next_date = debate_date.search(txt)
    if next_date:
        debate_list = get_daily_debates(txt=txt[next_date.end():], date=next_date)
        txt = txt[:next_date.start()]
    loops = most_loops
    debate_list.append([date, get_speeches(txt)])
    print(f'Processed {date.group(0)}')
    if most_loops > loops:
        global longest_day
        longest_day = date.group(0)
        print(f'Most strings! {most_loops}\n')
    return debate_list


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
                speeches.append(Speech(speaker, process_sentences(paragraphs)))
                paragraphs = []
                speaker = name.group(2)
                txt = txt[kaikōrero.end():]

        p, txt = get_paragraph(txt)
        paragraphs.append(p)
        if not txt:
            speeches.append(Speech(speaker, process_sentences(paragraphs)))
            break

    global most_loops
    if loops > most_loops:
        most_loops = loops

    return speeches


def process_sentences(paragraphs):
    # Check paragraph sentences to see if any contain te reo:
    utterances, reo, other = [], [], []
    consecutive_reo = consecutive_other = False
    has_tohutō = re.search('[āēīōūĀĒĪŌŪ]', ' '.join(paragraphs)) is not None
    for text in paragraphs:
        loop_flag = True
        while loop_flag:
            # Look for sentence / statement like endings and separate text:
            next_sentence = new_sentence.search(text)
            sentence = ''
            if next_sentence:
                sentence = text[:next_sentence.start() + 1]
                text = text[next_sentence.end():]
            else:
                sentence = text
                loop_flag = False

            # Check to see if sentence is te reo Māori:
            c, nums = kupu_ratios(sentence, tohutō=has_tohutō)
            sentence = clean_whitespace(sentence)
            length = nums['reo'] + nums['other'] + nums['ambiguous']
            # Add to list of consecutive Māori or non-Māori sentences,
            # Append parts of speech to the utterance list:
            if length > 2:
                if c:
                    reo.append(sentence)
                    if not consecutive_reo:
                        consecutive_reo = True
                        consecutive_other = False
                        utterances.append(Utterance(' '.join(other)))
                        other = []
                else:
                    other.append(sentence)
                    if not consecutive_other:
                        consecutive_other = True
                        consecutive_reo = False
                        utterances.append(Utterance(' '.join(reo)))
                        reo = []
    else:
        if consecutive_reo:
            utterances.append(Utterance(' '.join(reo)))
        elif consecutive_other:
            utterances.append(Utterance(' '.join(other)))
    return utterances


# Regex to replace page breaks with new line
page_break = re.compile('(\n{0,2}\d{1,2} [a-zA-Z]{3,9} \d{4}.*\n\n\f)')

# Regex to look for meeting date then split into date-debate key-value map
debate_date = re.compile(pattern=r'[A-Z]{6,9}, (\d{1,2}) ([A-Z]{3,9}) (\d{4})')

# Regex to check each paragraph matches for a new speaker, then extracts the name
new_speaker = re.compile('{titles}({speaker}|([^,\n]*\n){speaker})'.format(
    titles=f'(([-~{apostrophes}() a-zA-Z]*\n)*)', speaker='[^:\n]*:'))
name_behaviour = re.compile(
    '(\d{d}\. )?((Rt\.? )?(Hon\. )?([A-Z]([a-z{a}]+|[A-Z{a}]+|\.?))([ -{a}][tA-Z]([öa-z{a}]+|[ÖA-Z{a}]+|\.?))+)( \(|:)'.format(
        a=apostrophes, d='{1,2}'))


def tuhituhikifile(volume, debates, index_writer, corpus_writer):
    # Write te reo and day stats to file output:
    rā = māhina = tau = None
    switch1 = True
    switch539 = volume['name'] == 539

    for date, speeches in reversed(debates):
        r = date.group(1)
        r = 1 if (not r.isdigit() or int(r) < 1) else int(r) if int(r) <= 31 else 31
        m = date.group(2).lower()
        m = 1 if m not in inv_months else inv_months[m]
        t = date.group(3)
        if switch1:
            tau = t
            rā, māhina = r, m
            switch1 = False
        elif switch539 and t.isdigit() and t == 1994:
            tau = t
            rā, māhina = r, m
            switch539 = False
        elif 0 < m - māhina:
            rā, māhina = r, m
        elif 0 < r - rā:
            rā = r

        totals = {'reo': 0, 'ambiguous': 0, 'other': 0}
        i_row = {'url': volume['url'], 'volume': volume['name'], 'format': 'PDF', 'date1': date.group(0),
                 'date2': f'{tau}-{māhina}-{rā}'}
        c_row = {'utterance': 0}
        for k, v in i_row.items():
            c_row[k] = v
        for speech in speeches:
            for paragraph in speech.paragraphs:
                for k, v in paragraph.ratios.items():
                    if k != 'percent':
                        totals[k] += v

                c_row['utterance'] += 1
                if paragraph.condition and paragraph.ratios['reo'] > 2:
                    c_row.update({'text': paragraph.txt, 'speaker': speech.kaikōrero})
                    c_row.update(paragraph.ratios)
                    corpus_writer.writerow(c_row)
                    print('Volume {volume}: {date1}\n Utterance {utterance}: {speaker}\nMaori = {percent}%\n{text}\n'.format(
                        **c_row))

        i_row.update({'percent': get_percentage(**totals)})
        i_row.update(totals)
        index_writer.writerow(i_row)
        print('Maori = {reo}, Ambiguous = {ambiguous}, Non-Māori = {other}, Percentage = {percent} %'.format(**i_row))


def main():
    try:
        print('Processing PDF volumes 1987-2002:')
        process_txt_files(dirpath='1987-2002')
        print('PDF Corpus compilation successful')
    except Exception as e:
        raise e
    finally:
        print(f"--- Job took {get_rate()} ---")
        print(f'Looped through {most_loops} strings while processing {longest_day}')


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
            return f'{int(h)} hours {m} minutes {s} seconds'
        else:
            return f'{m} minutes {s} seconds'
    return f'{s} seconds'


if __name__ == '__main__':
    main()
