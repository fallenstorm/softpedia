"""
Дополнительная утилита для распаковки скачанных архивов и сохранения информации о файлах в них
"""

from pyunpack import Archive
import sqlite3
import asyncio
import uuid
import aiofiles
import aiohttp
import os
import shutil
import signal
from pathlib import Path

from contextlib import contextmanager

exts = ('.7z', '.bz2', '.Z', '.gz', '.rar', '.rz', '.tar', '.zip', '.jar', '.bz', '.xz', 'tbz2', 'txz', '.tgz')
like_cond = ' or '.join(list(map(lambda x: f"link like ?", exts)))

try:
    shutil.rmtree(f'/tmp/z')
except FileNotFoundError:
    pass
Path("/tmp/z").mkdir(parents=True, exist_ok=True)


class MyTimeout(Exception):
    pass


def raise_timeout(signum, frame):
    raise MyTimeout('Timeout')


@contextmanager
def timeout(sec):
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.alarm(sec)

    try:
        yield
    finally:
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


async def dl(url):
    dir = uuid.uuid4()
    ext = url.rsplit('.', 1)[1]
    print('EXT', ext)

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print('Status', resp.status, dir)
            if resp.status == 200:
                # создаем временную директорию, куда будем скачивать файлы
                Path(f"/tmp/z/{dir}").mkdir(parents=True, exist_ok=True)

                f = await aiofiles.open(f'/tmp/z/{dir}/source.{ext}', mode='wb')
                await f.write(await resp.read())
                await f.close()

                # распаковываем
                Path(f'/tmp/z/{dir}/unpaked').mkdir(parents=True, exist_ok=True)
                Archive(f'/tmp/z/{dir}/source.{ext}').extractall(f'/tmp/z/{dir}/unpaked')

                ret = []
                # проходим рекурсивно по всем директориям и возвращаем список файлов, их тип и размер в байтах
                for root, d, files in os.walk(f'/tmp/z/{dir}/unpaked'):
                    if not files:
                        continue

                    path = root.split('unpaked')[-1]

                    files_full = [f"{path}/{file} - {file.rsplit('.', 2)[-1]} - {os.path.getsize(f'{root}/{file}')}" for
                                  file in files]
                    files_str = '\n'.join(files_full)
                    ret.append(files_str)

                shutil.rmtree(f'/tmp/z/{dir}')
                print('\n'.join(ret))
                return resp.status, '\n'.join(ret)
            else:
                return resp.status, None


while True:
    try:
        with timeout(1):
            conn = sqlite3.connect('softpedia.db')
            c = conn.cursor()

            # Находим ссылки на архивы
            c.execute(f"select * from links where status is null and ({like_cond}) limit 1",
                      tuple(map(lambda x: f'%{x}%', exts)))
            row = c.fetchone()
            if not row:
                print('Done')
                break
            print('URL', row[1])

            try:
                status, files = asyncio.run(dl(row[1]))
            except MyTimeout:
                continue
            # если функция упала, записываем в бд 500 ошибку
            except Exception as err:
                print(err)
                status, files = 500, None

            c.execute(f'update links set status=?, files=? where id=?', (status, files, row[0]))
            conn.commit()
    # если бд залочена, то ждем
    except sqlite3.OperationalError:
        asyncio.sleep(10)
    finally:
        conn.close()
