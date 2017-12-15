import re
import sys
import argparse
from yelp_uri.encoding import recode_uri
from urllib.request import urlopen
from bs4 import BeautifulSoup

oropuare = "aāeēiīoōuū"
orokati = "hkmnprtwŋƒ"
pūriki_pākehā = "bcdfjlqsvxyz'"
papakupu_kī = "ABCDEFJHIJKLMNOPQRST"
no_tohutō = ''.maketrans({'ā': 'a', 'ē': 'e', 'ī': 'i', 'ō': 'o', 'ū': 'u'})
arapū = oropuare + orokati


def raupapa_tohu(kupu_hou):
    # Creates dictionaries for ordering the Māori alphabet from the vowel and consonant strings

    papakupu_whakamua = {kī: papakupu_kī[wāriutanga - 1]
                         for wāriutanga, kī in enumerate(arapū, 1)}
    papakupu_whakamuri = {
        papakupu_kī[kī - 1]: wāriutanga for kī, wāriutanga in enumerate(arapū, 1)}
    # Sorts into Māori alphabetical order
    for arapū_pūriki in arapū:
        kupu_hou = [re.sub(arapū_pūriki, papakupu_whakamua[arapū_pūriki], kupu)
                    for kupu in kupu_hou]

    kupu_hou.sort()

    for tau in range(len(arapū)):
        kupu_hou = [re.sub(
            papakupu_kī[tau], papakupu_whakamuri[papakupu_kī[tau]], kupu) for kupu in kupu_hou]

    return kupu_hou


def auaha_kupu_tūtira(kupu_tōkau):
    # Creates a list of all words in the file string that contain English and/or Māori letters, and one of all hyphenated words
    # kupu_tūtira = re.findall(r'[a-zāēīōū]+', kupu_tōkau, flags=re.IGNORECASE)

    # Keep English and Māori consistently
    kupu_pāhekoheko = re.findall('(?!-)(?!{p}*--{p}*)({p}+)(?<!-)'.format(
        p='[a-zāēīōū\-’\']'), kupu_tōkau, flags=re.IGNORECASE)

    # Don't uniquify
    # combines the lists, removes duplicates by transforming into a set and back again
    # kupu_tūtira_pīki = list(set(kupu_tūtita + kupu_pāhekoheko))
    return kupu_pāhekoheko


def poro_tūtira(kupu_hou, ignore_tohutō=False):
    # Removes words that contain any English characters from the string above
    # Set ignore_tohutō=True to become sensitive to the presence of macrons when making the match

    # if ignore_tohutō:
    #     kōnae = open("kupu_kino.txt", "r")
    #     kupu_pākehā = kōnae.read().split()
    #     kōnae.close()
    # else:
    #     kōnae = open("kupu_kino_no_tohutō.txt", "r")
    #     kupu_pākehā = kōnae.read().split()
    #     kōnae.close()

    # Replaces 'ng' and 'wh' with 'ŋ' and 'ƒ' respectively, since words with English characters have been removed and it is easier to deal with in unicode format
    kupu_hou = [re.sub(r'w\'', 'ƒ', re.sub(r'w’', 'ƒ', re.sub(
        r'ng', 'ŋ', re.sub(r'wh', 'ƒ', kupu)))) for kupu in kupu_hou]

    # Removes words that are English but contain Māori characters (like "the"), words that end in a consonant, words with a 'g' that is not preceeded by an 'n', words that have English characters and words that are in the stoplist of Māori-seeming english words.
    kupu_hou = [kupu for kupu in kupu_hou if not (re.compile("[{o}][{o}]".format(o=orokati)).search(
        kupu.lower()) or (kupu[-1].lower() in orokati) or ("g" in kupu.lower()) or any(
            pūriki in kupu.lower() for pūriki in pūriki_pākehā))]

    # kupu_hou = raupapa_tohu(kupu_hou)

    # Returns the letters to traditional format from unicode format
    kupu_hou = [re.sub(r'ŋ', 'ng', re.sub(r'ƒ', 'wh', kupu))
                for kupu in kupu_hou]

    return kupu_hou


def tatau_tupu(text):
    kupu_tūtira_pīki = auaha_kupu_tūtira(text)
    kupu_hou = poro_tūtira(kupu_tūtira_pīki)
    return len(kupu_hou), len(kupu_tūtira_pīki)


def dictionary_check_word(kupu_hou, ignore_tohutō=True):
    # Looks up a single word to see if it is defined in maoridictionary.co.nz
    # Set ignore_tohutō=False to not ignore macrons when making the match
    # Returns True or False

    kupu = kupu_hou.lower()
    if ignore_tohutō:
        kupu = kupu.translate(no_tohutō)
    search_page = recode_uri(
        'http://maoridictionary.co.nz/search?idiom=&phrase=&proverb=&loan=&histLoanWords=&keywords=' + kupu)
    page = urlopen(search_page)
    soup = BeautifulSoup(page, 'html.parser', from_encoding='utf8')

    titles = soup.find_all('h2')
    for title in titles[:-3]:
        title = title.text.lower()
        if "found 0 matches" in title:
            return False
            break
        elif kupu in (title.translate(no_tohutō).split() if ignore_tohutō else title.split()):
            return True
            break
    return False


def dictionary_check(kupu_hou, ignore_tohutō=True):
    # Looks up a list of words to see if they are defined in maoridictionary.co.nz
    # Set ignore_tohutō=False to become sensitive to the presence of macrons when making the match

    checks = list(map(dictionary_check_word, kupu_hou))

    good_list = [pair[1] for pair in zip(checks, kupu_hou) if pair[0]]
    bad_list = [pair[1] for pair in zip(checks, kupu_hou) if not pair[0]]
    return good_list, bad_list
