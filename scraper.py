
import datetime as dt
import re
import sqlite3
from urllib.request import urlopen

from lxml.html import document_fromstring as parse_html

base_url = 'http://www2.parliament.cy/parliamentgr/005_02.htm'

greek_months = {'Ιανουαρίου': 1,
                'Φεβρουαρίου': 2,
                'Μαρτίου': 3,
                'Απριλίου': 4,
                'Μαΐου': 5,
                'Ιουνίου': 6,
                'Ιουλίου': 7,
                'Αυγούστου': 8,
                'Σεπτεμβρίου': 9,
                'Οκτωβρίου': 10,
                'Νοεμβρίου': 11,
                'Δεκεμβρίου': 12}
number_match = re.compile(r'\(?Αρ\.\s*[Φφ]ακ\.\s*([0-9-.]+)\)\.?')


def parse_date(html):
    date, = html.xpath('//p[contains(string(.), "Συνεδρία Βουλής ημερομηνίας")]')
    date = date.text_content().replace('Συνεδρία Βουλής ημερομηνίας', '')
    d, m, y = date.split()
    return dt.date(int(y), greek_months[m], int(''.join(c for c in d if c.isdigit())))\
             .isoformat()


def parse_doc(doc, date, now):
    title, *other = (' '.join(i.text_content().split())
                     for i in doc.xpath('.//td'))
    return (number_match.search(title).group(1),
            number_match.sub('', title).strip(),
            *(*other, None)[:2],
            date,
            now)


def gather_docs(html, now):
    date = parse_date(html)
    for doc in (r
                for t in html.xpath('//p[contains(string(.), "ΚΕΦΑΛΑΙΟ Β΄")]'
                                    '/following-sibling::table')
                for r in t.xpath('.//tr[position() > 1]')):
        yield parse_doc(doc, date, now)


def main():
    with urlopen(base_url) as r:
        src = r.read().decode('windows-1253')

    now = dt.datetime.now().isoformat()
    con = sqlite3.connect('data.sqlite')
    with con:
        con.execute('''\
CREATE TABLE IF NOT EXISTS first_reading_archive
(src, time_last_scraped, UNIQUE (src))''')
        con.execute('''\
INSERT OR REPLACE INTO first_reading_archive VALUES (?, ?)''', (src, now))
    with con:
        con.execute('''\
CREATE TABLE IF NOT EXISTS first_reading
(number, title, sponsors, committees, date_tabled, time_last_scraped,
 UNIQUE (number, title, date_tabled))''')
        con.executemany('''\
INSERT OR REPLACE INTO first_reading VALUES (?, ?, ?, ?, ?, ?)''',
            gather_docs(parse_html(src), now))

if __name__ == '__main__':
    main()
