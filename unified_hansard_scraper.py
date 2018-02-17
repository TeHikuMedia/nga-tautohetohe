from nga_tautohetohe_hansard import ocr_html_scraper, ocr_text_cleaner, pdf_scraper, html_scraper


def main():
    ocr_html_scraper.main()
    ocr_text_cleaner.main()
    pdf_scraper.main()
    html_scraper.main()
    print('All te reo Hansard debates corpus aggregated')


if __name__ == '__main__':
    main()
