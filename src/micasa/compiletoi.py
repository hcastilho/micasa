import asyncio

import aiohttp
import bs4


max_concurrent_requests = 5

@asyncio.coroutine
def get(*args, **kwargs):
    response = yield from aiohttp.request('GET', *args, **kwargs)
    return (yield from response.read_and_close(decode=True))


aget = asyncio.coroutine(get)


def first_magnet(page):
    soup = bs4.BeautifulSoup(page)
    a = soup.find('a', title='Download this torrent using magnet')
    return a['href']


@asyncio.coroutine
def print_magnet(query):
    url = 'http://thepiratebay.se/search/{}/0/7/0'.format(query)
    with (yield from sem):
        page = yield from get(url, compress=True)
    magnet = first_magnet(page)
    print('{}: {}'.format(query, magnet))


distros = ['archlinux', 'ubuntu', 'debian']
sem = asyncio.Semaphore(max_concurrent_requests)
loop = asyncio.get_event_loop()
f = asyncio.wait([print_magnet(d) for d in distros])
loop.run_until_complete(f)
