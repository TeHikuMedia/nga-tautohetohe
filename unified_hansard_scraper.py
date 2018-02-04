import sys
sys.path.append(os.path.dirname(__file__) + "/nga_tautohetohe_hansard")
import historicalhtml_cleaner
import html_scraper
import pdf_scraper


def main():
    historicaltext_cleaner.main()
    pdf_scraper.main()
    html_scraper.main()


if __name__ == '__main__':
    main()
