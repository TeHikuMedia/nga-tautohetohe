from nga_tautohetohe_hansard import historicalhtml_scraper, historicaltext_cleaner, pdf_scraper, html_scraper


def main():
    historicalhtml_scraper.main()
    historicaltext_cleaner.main()
    pdf_scraper.main()
    html_scraper.main()
    print('All te reo Hansard debates corpus aggregated')


if __name__ == '__main__':
    main()
