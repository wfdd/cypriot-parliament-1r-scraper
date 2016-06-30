
import datetime as dt
import itertools as it
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
number_match = re.compile(r'\(Αρ\.\s+Φακ\.\s+([0-9-.]+)\)\.?')


def parse_date(html):
    date, = html.xpath('//p[contains(string(.), "Συνεδρία Βουλής ημερομηνίας")]')
    date = date.text_content().replace('Συνεδρία Βουλής ημερομηνίας', '')
    d, m, y = date.split()
    return '{}-{:02d}-{:02d}'.format(*map(int, (y, greek_months[m], d)))


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
    docs = it.chain.from_iterable(
        t.xpath('.//tr[position() > 1]')
        for t in html.xpath('//p[contains(string(.), "ΚΕΦΑΛΑΙΟ Β΄")]'
                            '/following-sibling::table'))
    for doc in docs:
        yield parse_doc(doc, date, now)


def main():
    with urlopen(base_url) as r, sqlite3.connect('data.sqlite') as c:
        src = r.read().decode('windows-1253')
        now = dt.datetime.now().isoformat()
        c.execute('''\
CREATE TABLE IF NOT EXISTS first_reading
(number, title, sponsors, committees, date_tabled, time_last_scraped,
 UNIQUE (number, title, date_tabled))''')
        c.executemany('''\
INSERT OR REPLACE INTO first_reading VALUES (?, ?, ?, ?, ?, ?)''',
            gather_docs(parse_html(src), now))
        c.execute('''\
CREATE TABLE IF NOT EXISTS first_reading_archive
(src, time_last_scraped, UNIQUE (src))''')
        c.execute('''\
INSERT OR REPLACE INTO first_reading_archive VALUES (?, ?)''', (src, now))

if __name__ == '__main__':
    main()
