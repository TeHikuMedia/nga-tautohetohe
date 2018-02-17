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
volumeindex_fieldnames = ['retrieved', 'url', 'name', 'period', 'session', 'downloaded', 'processed']
rāindex_fieldnames = ['url', 'volume', 'date', 'reo', 'ambiguous', 'other', 'percent', 'retrieved', 'format',
                      'incomplete']
reo_fieldnames = ['url', 'volume', 'date', 'utterance', 'speaker', 'reo', 'ambiguous', 'other', 'percent', 'text']


class Speech:
    """This class stores the speaker name and the paragraphs of the speaker's speech."""

    def __init__(self, kaikōrero, paragraphs):
        self.kaikōrero = kaikōrero
        self.paragraphs = paragraphs


class Paragraph:
    """This class stores a paragraph of text as well is numerical information about the text."""

    def __init__(self, txt):
        self.txt = txt
        self.condition, self.ratios = kupu_ratios(txt)


# Regex to replace page breaks with new line
page_break = re.compile('(\n{0,2}\d{1,2} [a-zA-Z]{3,9} \d{4}.*\n\n\f)')


def process_txt_files(dirpath):
    # Create output files if not exists:
    if not exists(rāindexfilename):
        with open(rāindexfilename, 'w') as f:
            csv.DictWriter(f, rāindex_fieldnames).writeheader()
    if not exists(corpusfilename):
        with open(corpusfilename, 'w') as f:
            csv.DictWriter(f, reo_fieldnames).writeheader()

    # Open output files and get ready to extract and write volume information:
    with open(rāindexfilename, 'a') as i, open(corpusfilename, 'a') as c:
        index_writer = csv.DictWriter(i, rāindex_fieldnames)
        corpus_writer = csv.DictWriter(c, reo_fieldnames)

        # Iterate through volume file list extracting te reo corpus and information about each day of debates:
        for f, v in get_file_list(dirpath):
            print('\nProcessing {}:\n'.format(f))

            # Read from volume text files
            with open('{}/{}'.format(dirpath, f), 'r') as hansard_txt:
                txt = sub_vowels(page_break.sub('\n', hansard_txt.read()))
                txt = re.sub(r'\[[^\]]*]', '', txt)

                # Sort through text with RegEx then write output:
                tuhituhikifile(v, get_daily_debates(txt), index_writer, corpus_writer)

            # Update record of processed volumes:
            v_rows = []
            with open(volumeindex_filename, 'r') as vol_file:
                reader = csv.DictReader(vol_file)
                for row in reader:
                    if row['name'] == v['name']:
                        row['processed'] = row['downloaded]'] = True
                    v_rows.append(row)
            with open(volumeindex_filename, 'w') as vol_file:
                writer = csv.DictWriter(vol_file, volumeindex_fieldnames)
                writer.writeheader()
                writer.writerows(v_rows)
            print('{} processed at {} after {}\n'.format(f, datetime.now(), get_rate()))


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
    switch1 = switch2 = False

    # Scrape meta data from table list of Hansard volumes
    for tr in bs(urlopen('https://www.parliament.nz/en/pb/hansard-debates/historical-hansard/'), 'html.parser').select(
            '.wikitable')[0]('tr'):
        # Sort data from each cell of each row of table list into list of dictionaries
        row = {}
        row_cells = tr('td')
        switch3 = False
        for cell in row_cells:
            if cell.a:
                name = cell.get_text(strip=True)
                if switch1:
                    switch2 = not (not name.isdigit() or int(name) < 606)
                    if switch2:
                        break
                    else:
                        row['name'] = name
                        row['url'] = cell.a['href']
                        row['retrieved'] = datetime.now()
                else:
                    switch1 = name == last_index
                    break
            else:
                if switch3:
                    row['session'] = cell.get_text().strip()
                else:
                    row['period'] = cell.string.strip()
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
        with open(volumeindex_filename, 'r') as v_index:
            reader = csv.DictReader(v_index)
            for row in reader:
                rows.append(row)
        last_entry = row[-1]['name']

        # Scrape remaining volume urls from parliament website & save to file if the index doesn't have them yet:
        if not last_entry.isdigit() or int(last_entry) < 606:
            with open(volumeindex_filename, 'a') as v_index:
                writer = csv.DictWriter(v_index, volumeindex_fieldnames)
                entries = scrape_volume_urls(last_entry)
                writer.writerows(entries)
                rows.extend(entries)

        return rows


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
        debate_list = get_daily_debates(txt=txt[nextdate.end():], date=nextdate)
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
name_behaviour = re.compile(
    '(\d{d}\. )?((Rt\.? )?(Hon\. )?([A-Z]([a-z{a}]+|[A-Z{a}]+|\.?))([ -{a}][tA-Z]([öa-z{a}]+|[ÖA-Z{a}]+|\.?))+)( \(|:)'.format(
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
        i_row = {'url': volume['url'], 'volume': volume['name'], 'date': date}
        c_row = {'utterance': 0}
        for k, v in i_row.items():
            c_row[k] = v
        for speech in speeches:
            for paragraph in speech.paragraphs:
                c_row['utterance'] += 1
                if paragraph.condition:
                    c_row.update({'text': clean_whitespace(paragraph.txt), 'speaker': speech.kaikōrero})
                    c_row.update(paragraph.ratios)
                    print('Volume {volume}: {date}\n{speaker}: {utterance},\nMaori = {percent}%\n{text}\n'.format(
                        **c_row))
                    corpus_writer.writerow(c_row)

                for k, v in paragraph.ratios.items():
                    if k != 'percent':
                        totals[k] += v
        i_row.update({'percent': get_percentage(**totals), 'format': 'PDF'})
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
        print("--- Job took {} ---".format(get_rate()))
        print('Looped through {} strings while processing {}'.format(most_loops, longest_day))


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
