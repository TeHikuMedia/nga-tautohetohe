from taumahi import tatau_tupu
import csv
import sys
import argparse
import re


def clean_csv(corpusinputname, cleancorpusoutput):
    with open(corpusinputname, 'r') as incorpus:

        with open(cleancorpusoutput, 'w') as outcorpus:
            writer = csv.DictWriter(outcorpus, [
                'Hansard document url',
                'wā',
                'title',
                'section number',
                'utterance number',
                'ingoa kaikōrero',
                'is Māori (%)',
                'kōrero waenga'
            ])
            writer.writeheader()
            for row in csv.DictReader(incorpus):
                if is_te_reo(row['kōrero waenga']):
                    writer.writerow(row)


def is_te_reo(kōrero_waenga):
    num_Māori, num_ambiguous, size = tatau_tupu(kōrero_waenga)

    if size:
        heMāori = (num_Māori / size) * 100

    save_corpus = heMāori > 50
    if not save_corpus:
        save_corpus = re.match(
            r'\[Authorised Te Reo text', kōrero_waenga)
        if save_corpus:
            awaiting_teReo = True
    elif size <= 10:
        save_corpus = not any(word in kōrero_waenga for word in [
            'take', 'Take', 'too', 'Too', 'woo', 'hoo', 'No', 'no', 'Ha', 'ha', 'name', 'one', 'where', 'who', 'We', 'we', 'Nowhere', 'nowhere', 'are', 'he', 'hero', 'here', 'none', 'whoa'])

    return save_corpus


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', '-i', help="Input multilanguage corpus CSV file")
    parser.add_argument(
        '--output', '-o', help="Output cleaned CSV file")

    args = parser.parse_args()

    clean_csv(args.input, args.output)


if __name__ == '__main__':
    main()
