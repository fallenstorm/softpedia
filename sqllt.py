import sqlite3
import re
import time
import signal
from splinter import Browser
from contextlib import contextmanager

conn = sqlite3.connect('softpedia.db')

c = conn.cursor()
soft_url = 'https://www.softpedia.com/'
dl = {}
exts = (
    '.exe', '.7z', '.bz2', '.Z', '.gz', '.rar', '.rz', '.tar', '.zip', '.run', '.tgz', '.tbz2', '.bin', 'tbz2', 'txz',
    '.jar', '.rpm', 'iso', 'deb', '.bz', '.xz', '.msi', '.cav', '.dmg', '.pkg', '.ipsw')

# Create table
c.execute('''CREATE TABLE if not exists urls
             (id INTEGER PRIMARY KEY, url text UNIQUE, checked bool, title text, cnt integer)''')
c.execute('''CREATE TABLE if not exists links
             (id INTEGER PRIMARY KEY, link text UNIQUE, status integer, files text)''')

c.close()


class MyTimeout(Exception):
    pass


def raise_timeout(signum, frame):
    raise MyTimeout


@contextmanager
def timeout(sec):
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.alarm(sec)

    try:
        yield
    finally:
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


while True:
    conn = sqlite3.connect('softpedia.db')

    c = conn.cursor()
    try:
        # если браузер подвис, то убиваем его
        with timeout(300):
            # сначала пытаем достать ссылки, где есть конечные ссылки на дистрибутив
            c.execute("select * from urls where url like '%#download%' and checked=0 limit 1")
            row = c.fetchone()

            if isinstance(row, tuple):
                url = row[1]
                _id = row[0]
            # если не получилось, выбираем любую другую непроверенную
            else:
                c.execute('SELECT * FROM urls WHERE checked=0 limit 1')
                row = c.fetchone()

                print('Row', row)
                if isinstance(row, tuple):
                    print('tuple')
                    url = row[1]
                    _id = row[0]
                # если и такой нет, то берем корневую ссылку(скорее всего, первый запуск)
                else:
                    url = soft_url
                    _id = c.execute("INSERT INTO urls (url, checked, title) VALUES (?, 0, 'Root')", (url,))
                    _id = _id.lastrowid
            print('ID', _id)

            print('URL', url)

            # with Browser('chrome', headless=True, options=chrome_options) as br:
            with Browser('chrome', headless=True) as br:
                br.wait_time = 10
                br.visit(url)
                # если страница загрузки
                if '#download' in url and 'index' not in url:
                    # ищем временные ссылки на страницу с прямой ссылкой
                    links = br.links.find_by_partial_href('postdownload')
                    links = [link['href'] for link in links]
                    if not links:
                        # если забанило, то спим
                        print('Ban, sleeping')
                        time.sleep(300)
                    for link in links:
                        # теперь проходим по каждой ссылке и находим прямую на дистрибутив
                        br.visit(link)
                        texts = br.links.find_by_text(' CLICK TO START IT MANUALLY')
                        # чтобы не забанило
                        time.sleep(1)
                        for text in texts:
                            # проверяем, что она внешняя и вставляем в бд
                            if text['href'].startswith('http') and 'softpedia' not in text['href'] \
                                    and any(substring in text['href'] for substring in exts) and not text[
                                'href'].endswith('/download'):
                                print('FIN', text['href'])
                                c.execute("INSERT OR IGNORE INTO links (link) VALUES (?)", (text['href'],))
                    c.execute('update urls set cnt=? where id=?', (len(links), _id))
                else:
                    links = br.find_by_tag('a')
                    for link in links:
                        print('LINK', link['href'], link['title'])
                        if not link['href'] or not link['title']:
                            continue

                        elif 'softpedia.com' in link['href'] \
                                and 'news.softpedia.com' not in link['href'] \
                                and 'webapps.softpedia.com' not in link['href'] \
                                and 'mobile.softpedia.com' not in link['href'] \
                                and '.xml' not in link['href'] \
                                and '.jpg' not in link['href'] \
                                and '.png' not in link['href'] \
                                and link['href'].startswith('http'):
                            # финт, добавляем к нужным ссылкам #download, чтобы они брались в первую очередь
                            if 'softpedia.com/get/' in link['href'] and re.search('\.s?html$', link['href']):
                                href = link['href'] + '#download'
                            else:
                                href = link['href']

                            print('NEW!')
                            c.execute("INSERT OR IGNORE INTO urls (url, checked, title) VALUES (?, 0, ?)",
                                      (href, link['title']))
                    c.execute('update urls set cnt=? where id=?', (len(links), _id))

            # помечаем, что ссылка проверена
            c.execute(f'update urls set checked=1 where id={_id}')
            conn.commit()
    except Exception:
        import traceback

        print(traceback.format_exc())
        conn.rollback()
    finally:
        conn.close()
