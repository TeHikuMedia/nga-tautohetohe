# import libraries
import csv
import time
import re
from taumahi import *
from os import listdir
from os.path import isfile, join, exists

indir = '1854-1987'
volumeindex_filename = 'hansardvolumeindex.csv'
rāindexfilename = 'hansardrāindex.csv'
corpusfilename = 'hansardreomāori.csv'
volumeindex_fieldnames = ['retrieved', 'url', 'name', 'period', 'session', 'format', 'downloaded', 'processed']
dayindex_fieldnames = ['retrieved', 'url', 'volume', 'format', 'date1', 'date2', 'reo', 'ambiguous', 'other', 'percent',
                       'incomplete']
reo_fieldnames = ['url', 'volume', 'format', 'date1', 'date2', 'utterance', 'speaker', 'reo', 'ambiguous', 'other',
                  'percent', 'text']
hathi_domain = 'https://babel.hathitrust.org'

# Vars for generating clean numeric dates from OCRed dates:
months = {1: 'january', 2: 'february', 3: 'march', 4: 'april', 5: 'may', 6: 'june', 7: 'july', 8: 'august',
          9: 'september', 10: 'october', 11: 'november', 12: 'december'}
inv_months = {v: k for k, v in months.items()}
rā = māhina = tau = None


def process_csv_files():
    # Make output files if not exist:
    if not exists(rāindexfilename):
        with open(rāindexfilename, 'w', newline='', encoding='utf8') as f:
            writer = csv.DictWriter(f, dayindex_fieldnames)
            writer.writeheader()
    if not exists(corpusfilename):
        with open(corpusfilename, 'w', newline='', encoding='utf8') as f:
            writer = csv.DictWriter(f, reo_fieldnames)
            writer.writeheader()

    # Process list of unprocessed volumes:
    for result in map(process_csv, get_file_list()):
        pass


def get_file_list():
    volume_list = read_index_rows()
    file_list = [f for f in listdir(indir) if isfile(join(indir, f)) and f.endswith('.csv')]

    for v in volume_list:
        for f in file_list:
            if v['name'] == f[:f.index('.csv')] and not v['processed']:
                yield f, v


def read_index_rows():
    while True:
        rows = []
        with open(volumeindex_filename, 'r', newline='', encoding='utf8') as v_index:
            reader = csv.DictReader(v_index)
            for row in reader:
                if not row['name'].isdigit() or int(row['name']) < 483:
                    rows.append(row)
            return rows


def process_csv(args):
    f, v = args
    print(f'Extracting corpus from {f}:')

    # Process the volume:
    Volume(f, v).process_pages()

    # Update the record of processed volumes:
    rows = []
    for row in read_index_rows():
        if v['name'] == row['name']:
            row['processed'] = True
        rows.append(row)

    while True:
        with open(volumeindex_filename, 'w', newline='', encoding='utf8') as v_index:
            writer = csv.DictWriter(v_index, volumeindex_fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        break

    return v['name']


class Volume(object):
    """This class loads the pages of a volume from a csv and sorts through the text to extract kōrero reo Māori &
    numerical information."""

    def __init__(self, filename, v):
        # Initialise instance and store some associated meta data:
        self.filename = filename
        self.day, self.speech, self.totals = {}, {'utterance': 0}, {'reo': 0, 'ambiguous': 0, 'other': 0}
        self.day['format'] = self.speech['format'] = 'OCR'
        self.day['volume'] = self.speech['volume'] = v['name']
        self.day['url'] = self.speech['url'] = v['url'] if v['url'].startswith('https') else f'{hathi_domain}{v["url"]}'
        if 'retrieved' in v:
            self.day['retrieved'] = v['retrieved']
        else:
            self.day['retrieved'] = v['retreived']
        self.flag294 = int(self.speech['volume'].isdigit() and int(self.speech['volume']) >= 294)
        self.flag410 = int(self.flag294 and int(self.speech['volume']) >= 410)

        date = re.match('(\d{1,2}) ([a-zA-Z]+) (\d{4})', v['period'])
        global rā, māhina, tau
        rā = int(date.group(1))
        māhina = inv_months[date.group(2).lower()]
        tau = int(date.group(3))
        self.day['date1'] = self.speech['date1'] = date.group(0)
        self.speech['date2'] = self.day['date2'] = f'{tau}-{māhina}-{rā}'
        self.same_day_flag = False

    def process_pages(self):
        """Invoke this method from a class instance to process the debates."""

        # Open volume csv & read row pages:
        with open(f'{indir}/{self.filename}', 'r', newline='', encoding='utf8') as kiroto:
            reader = csv.DictReader(kiroto)
            day = []  # day list will hold pages of text
            for page in reader:
                if not (page['url'].endswith(('c', 'l', 'x', 'v', 'i')) or page['page'] == '1') and re.search(
                        '[a-zA-Z]', page['text']):
                    day = self.__process_page(page, day)

    def __process_page(self, page, day):
        text = page['text']
        looped = 0
        global rā, māhina, tau

        while True:
            next_day = date_pattern[self.flag294].search(text)
            if next_day and not next_day.group(0).startswith('Swainson'):
                header = None if looped else header_pattern.match(text[:next_day.start()])
                previoustext = text[:next_day.start()] if not header else text[header.end():next_day.start()]
                # if not looped:
                #     header = header_pattern.match(text[:next_day.start()])
                # if header:
                #     previoustext = text[header.end():next_day.start()]
                # else:
                #     previoustext = text[:next_day.start()]
                if previoustext:
                    day.append(previoustext.strip())
                self.__process_day(day)

                # Generate numerical date from the regex match and check if actually is a later date:
                self.same_day_flag = False
                r = next_day.group(1)
                r = 1 if (not r.isdigit() or int(r) < 1) else int(r) if int(r) <= 31 else 31
                m = next_day.group(2).lower()
                m = 1 if m not in inv_months else inv_months[m]
                t = next_day.group(3)
                if re.match('[ABCDE]'|'261', self.day['volume']) and t.isdigit() and int(t) - tau == 1:
                    tau += 1
                    rā, māhina = r, m
                elif 0 < m - māhina:
                    rā, māhina = r, m
                elif 0 < r - rā:
                    rā = r
                else:
                    self.same_day_flag = True

                # Write day statistics:
                if not self.same_day_flag and sum(self.totals.values()) > 50:
                    self.day['percent'] = get_percentage(**self.totals)
                    self.day.update(self.totals)
                    with open(rāindexfilename, 'a', newline='', encoding='utf8') as output:
                        writer = csv.DictWriter(output, dayindex_fieldnames)
                        writer.writerow(self.day)

                # Reset page list and day totals, get meta info for next day:
                day = []
                if not self.same_day_flag or sum(self.totals.values()) <= 50:
                    self.totals = {'reo': 0, 'ambiguous': 0, 'other': 0}
                    self.day['date1'] = clean_whitespace(next_day.group(0))
                    self.speech['date2'] = self.day['date2'] = f'{tau}-{māhina}-{rā}'
                    self.day['url'] = page['url'] if page['url'].startswith('https') else '{}{}'.format(hathi_domain,
                                                                                                        page['url'])
                    self.day['retrieved'] = page['retrieved'] if ('retrieved' in page) else page['retreived']
                    self.speech['utterance'] = 0

                self.speech['date1'] = clean_whitespace(next_day.group(0))
                self.speech['url'] = page['url'] if page['url'].startswith('https') else '{}{}'.format(hathi_domain,
                                                                                                       page['url'])
                text = text[next_day.end():]
                looped += 1
            else:
                # No more dates in page, append rest of text to page list and return list:
                if not looped:
                    header = header_pattern.match(text)
                    if header:
                        text = text[header.end():]
                if text:
                    day.append(text.strip())
                return day

    def __process_day(self, day):
        # Join pages and remove hyphenated line breaks
        text = re.sub('(?<=[a-z]) *-\n+ *(?=[a-z])', '', '\n'.join(day))

        # Remove name lists, ayes and noes
        # Remove lines with no letters, short lines, single word lines and lines of punctuation, capitals, digits
        regex = ['([A-Z][ a-zA-Z.]+, ){2}[A-Z][ a-zA-Z.]+\.', '(AYE|Aye|NOE|Noe)[^\n]*', '[^A-Za-z]*', '[^\n]{1,2}',
                 '[ \-\d,A-Z.?!:]+', '[a-zA-Z]+', ]
        for r in regex:
            text = re.sub('(?<=\n){}\n'.format(r), '', text)

        self.__process_paragraphs(text)

    def __process_paragraphs(self, text):
        utterance = []
        while True:
            # Look for paragraph endings and separate text:
            p_break = paragraph_pattern.search(text)
            if p_break:

                utterance = self.__process_paragraph(text[:p_break.start()], utterance)

                text = text[p_break.end():]
            else:
                utterance = self.__process_paragraph(text, utterance)
                if utterance:
                    self.__write_row(utterance)
                break

    def __process_paragraph(self, text, utterance):
        # Check to see if paragraph declares name of speaker:
        kaikōrero = newspeaker_pattern[self.flag410].match(text)
        if kaikōrero:
            name = kaikōrero.group(1)
            if name:
                # If new speaker then write any previous te reo speech and clear list for new speech:
                if utterance:
                    self.__write_row(utterance)
                    utterance = []
                self.speech['speaker'] = clean_whitespace(name)
                text = text[kaikōrero.end():]

        # Build up list of consecutive te reo sentences and return:
        return self.__process_sentences(text, utterance)

    def __process_sentences(self, text, utterance):
        consecutive = {'reo': True} if utterance else {'reo': False}
        consecutive['other'] = False
        loop_flag, nums = True, {}

        while loop_flag:
            # Look for sentence / statement like endings and separate text:
            next_sentence = new_sentence.search(text)
            if next_sentence:
                sentence = text[:next_sentence.start() + 1]
                text = text[next_sentence.end():]
            else:
                sentence = text
                loop_flag = False

            # Check to see if sentence is te reo Māori and build list of consecutive sentences if so:
            c, nums = kupu_ratios(sentence, tohutō=False)
            sentence = clean_whitespace(sentence)
            if c:
                if consecutive['reo']:
                    utterance.append(sentence)
                else:
                    consecutive['reo'] = True
                    consecutive['other'] = False
                    self.speech['utterance'] += 1
                    utterance = [sentence]

            else:
                # Check to see if consecutive condition broken
                # if so then write the previous te reo speech to output and clear list:
                if not consecutive['other']:
                    if utterance:
                        self.__write_row(utterance)
                    utterance = []
                    consecutive['other'] = True
                    consecutive['reo'] = False
                    self.speech['utterance'] += 1

                # Record nums count if text is part of speech:
                first_letter = re.search('[a-zA-Z£]', sentence)
                if first_letter:
                    sentence = sentence[first_letter.start():]
                    if 5 < len(sentence):
                        bad_egg = re.match(
                            '([^ A-Z]+ )?[A-Z][^ ]*(([^a-zA-Z]+[^ A-Z]*){1,2}[A-Z][^ ]*)*(([^a-zA-Z]+[^ A-Z]*){2})?',
                            sentence)
                        if not bad_egg:
                            bad_egg = re.match('([^ ]{1,3} )+[^ ]{1,3}', sentence)

                        if not (bad_egg and bad_egg.group(0) == sentence):
                            for k, v in nums.items():
                                if k != 'percent':
                                    self.totals[k] += v

            # Only returns list of consecutive te reo Māori sentences:
            if not loop_flag:
                return utterance

    def __write_row(self, text):
        text = ' '.join(text)
        first_letter = re.search('[a-zA-Z£]', text)
        length = len(text)
        # Filter garbage and save results if good:
        if first_letter and length > 3:
            text = text[first_letter.start():]
            bad_egg = re.match(
                '([^ A-Z]+ )?[A-Z][^ ]*(([^a-zA-Z]+[^ A-Z]*){1,2}[A-Z][^ ]*)*(([^a-zA-Z]+[^ A-Z]*){2})?', text)
            if not bad_egg:
                bad_egg = re.match('([^ ]{1,3} )+[^ ]{1,3}', text)

            if not (bad_egg and bad_egg.group(0) == text):
                c, nums = kupu_ratios(text, tohutō=False)
                for k, v in nums.items():
                    if k != 'percent':
                        self.totals[k] += v

                # and not (nums['reo'] < 5 and nums['other'] + nums['ambiguous'] < 10):
                if c and nums['reo'] > 2 and nums['other'] < 20:
                    self.speech['text'] = text
                    self.speech.update(nums)
                    print(self.speech['text'])
                    with open(corpusfilename, 'a', newline='', encoding='utf8') as output:
                        writer = csv.DictWriter(output, reo_fieldnames)
                        writer.writerow(self.speech)


# New header pattern from volume 359 onwards (5 Dec 1968), 440 onwards - first 3 lines, 466 onward - 1 line
header_pattern = re.compile(
    '[^\n]*\n((([^\n\]]*\n){0,5}[^\n]*\][^\n]*)\n)?((([^ \n]+( [^ \n,—]+){0,3}))\n)*(([^a-z]([^\n:—](?!([^a-zA-Z]+[a-z]+){3}))*( (?!O )[^a-z\n][^ —:\n]*){2}[^\-\n:]\n)+)*')
# best catch-all header pattern so far:
# ',"[^\n]*\n((([^\n\]]*\n){0,5}[^\n]*\][^\n]*)\n)?((([^ \n]+( [^ \n,—]+){0,3}))\n)*(([^a-z]([^\n:—](?!([^a-zA-Z]+[a-z]+){3}))*( (?!O )[^a-z\n][^ —:\n]*){2}[^-\n:]\n)+)*'


# Regex to look for meeting date. Date pattern changes from vol 294 onwards
# day = group(1), month = group(2), year = group(3) if explicitly stated
date_pattern = [
    re.compile('\n[A-Z][a-z]{5,8}, ([\dinISl&^]{1,2})[a-zA-Z]{2} ([A-Z][!1Ia-z]{2,8}), ([\d(A-Z]{4,5})[^\n]{0,4}\n'),
    re.compile('\n[A-Z][A-Za-z]{5,8}, (\d{1,2}) ([A-Za-z]{3,9}),? ?(\d|[^\n–:!?]){4}?[^\n–:!?]{0,4}\n')]

# Speaker pattern changes at volume 410 (19 May 1977). Pre-410 many passages are written
# as a narrative, so will process it as whole paragraphs.
newspeaker_pattern = [re.compile(
    '[^a-zA-Z\n]*([A-Z][^—:\n]*( ?[A-Z]){3,}(\s*\([a-zA-Z\s]*\))?)(((\.? ?—\-?)\s*(?=[A-Z£]))|[^a-zA-Z]+(?=said|asked|wished|did|in|replied|hoped|was|thought|supported|desired|obtained|moved|having|by|brought|seconded|announ(c|e)ed))'),
    re.compile('([A-Z][^\n]*[)A-Z])(\s+replied)?[:;]')]


# Previous versions:
# NEW vs
# '(([-~‘’\'() a-zA-Z]*\n)*)([^:\n]*:|([^,\n]*\n)[^:\n]*:)'
# '((\d{d}\.|The) )?(((Rt\.?|Right) )?(Hon\. )?(Mr\. )?([A-Z]([a-z{a}]+|[A-Z{a}]+|\.?))([ -{a}][tA-Z]([öa-z{a}]+|[ÖA-Z{a}]+|\.?))+)([-—]+| \(|:)'.format(a=apostrophes, d='{1,2}')
# name_behaviour = '((\d{1,2}\.|The) )?(((Rt\.?|Right) )?(Hon\. )?(Mr\. )?([A-Z]([a-z‘’\']+|[A-Z‘’\']+|\.?))([ -‘’\'][tA-Z]([öa-z‘’\']+|[ÖA-Z‘’\']+|\.?))+)([-—]+| \(|:)'
# old vs
# '([A-Z .:—-]*\n)*[A-Z]([^(\n]+\([^-—\n]+[-—]*\n)?[a-zA-Z". ()]+\. ?[-—]+(?!\n)'


def main():
    try:
        print('Processing text from volumes 1854-1987:')
        process_csv_files()
        print('Corpus aggregation successful')
    except Exception as exception:
        raise exception
    finally:
        print(f"--- Job took {get_rate()} ---\n")


start_time = time.time()


def get_rate():
    m, s = divmod(time.time() - start_time, 60)
    s = int(s)
    h, m = divmod(m, 60)
    if m:
        m = int(m)
        return f'{int(h)} hours {m} minutes {s} seconds' if h else f'{m} minutes {s} seconds'

    return f'{s} seconds'


if __name__ == '__main__':
    main()
