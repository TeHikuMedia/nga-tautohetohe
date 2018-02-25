# nga-tautohetohe

This project relies on taumahi.py which can be downloaded from Te Hiku Media [nga-kupu](https://github.com/TeHikuMedia/nga-kupu) git repo.
There are two installation options:
1. Run the setup file
2. Copy taumahi.py and taumahi_tūtira directly into the same directory as the unified_hansard_scraper.py

This project searches through all the NZ Parliament Hansard debates since 1854 until the present day for strings of text that kōrero te reo Māori.
The Hansard debates volumes can be accessed online via the official [NZ Parliament website](https://www.parliament.nz/en/pb/hansard-debates/historical-hansard/).

The Hansard volumes come in three main digital formats deriving from three different eras of publishing technology:
1. The first 488 volumes from beginning of the reports in 1854 until 1987 are OCR scans from analogue printing.
The style of writing in the Hansard reports up to volume 409 (28 February 1977) is often in a narrative format in English and does not always directly quote everything that each MP said in parliament, whereas every record from volume 410 onwards only records direct quotations of what each speaker said. It is possible (perhaps a Hansard expert could better clarify) that te reo Māori was spoken on various occasions but the intent of the speech was only recorded in English narrative.
For the most part letters and words have been interpreted correctly by the OCR program.
However the OCR scans are not perfect.
There are frequent OCR mistakes resulting in many incorrectly spelled words which reduce the overall quality of the digital text, especially in the earlier volumes. 
2. Volumes 483 - 605 between 1987 - 2002 are PDFs produced from computer word processing software.
The PDFs are available from a public [Google Drive folder](https://drive.google.com/drive/folders/0B1Iwfzv-Mt3CRGZkMWNf).
The Hansard reports also begin using macronised kupu from 1994 onwards (volume 539).
3. All daily debates from 2003 onwards can be accessed online as HTML formatted webpages [here](https://www.parliament.nz/en/pb/hansard-debates/rhr/). Volume compilations can also be downloaded as PDFs from another Google Drive folder [here](https://drive.google.com/drive/folders/0B1Iwfzv-Mt3CWWN5ZVEyQVYyNWM).

A different script has been written to sort through the text and extract te reo for each of these formats.
Each of these scripts is found in the sub-folder 'nga_tautohetohe_hansard'.
A unified_hansard_scraper.py has been written to run each of these scripts sequentially in the order of the Hansard volumes.
The script always picks up where from where it last got up too, so no worries if you cancel the programme part way through then rerun later.

Downloading and processing the first 488 volumes will take 1-3 days due to very slow download speed from the server where they are stored, whereas downloading and processing the debates from 1987 onwards will take about 1-3 hours.
TODO: Upload all OCR volume text into a Google Drive folder for faster download. 

Python 3.6 or later is required to run the code due to string formatting in the code.

Before running the unified_hansard_scraper you must also manually download the 1987-2002 PDFs and convert them to text files.
1. Download and store the pdfs in a folder called '1987-2002' beside unified_hansard_scraper.py.
2. pdf_scraper.py has been written to work on the output from the pdftotext converter available as a package for linux terminals.
Using any other pdf to text converter will only work with pdf_scraper.py
if it produces text with the same syntax as pdftotext.
To convert the pdftotext using a linux system such as ubuntu, go into the PDF folder and run the following from a terminal:

        for f in *.pdf; do   pdftotext "$f"; done
3. Run unified_hansard_scraper.py
4. Return in 3 days time. :D