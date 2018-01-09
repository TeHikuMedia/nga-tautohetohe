# nga-tautohetohe

This project relies on taumahi.py which can be downloaded and installed from https://github.com/TeHikuMedia/nga-kupu

hansard_WEBscraper.py scrapes HTML text from the Hansard report webpages using BeautifulSoup4 for the years 2003 - present.
Simply run the script and observe it scrape the Hansard webpages and output te reo Māori into a csv file.

hansard_PDFscraper.py processes txt files of the Hansard reports for years 1987 - 2002. The Hansard volumes can be downloaded in PDF format from https://drive.google.com/open?id=0B1Iwfzv-Mt3CRGZkMWNfeXoybmcproduced
Place the pdfs in a folder called '1987-2002' and from a shell run:

for f in *.pdf; do   pdftotext "$f"; done

to generate text files from the pdfs.

from pdf2txt conversionscrapes HTML text from the Hansard report webpages using BeautifulSoup4 for the years 2003 - present.
Simply run the script and observe it scrape the Hansard webpages and produce output into csv file.
