
import asyncio
import logging
from urllib.parse import urljoin, urlencode

import aiohttp
from lxml import html


CONSUMERS = 1
DEBUG = True


class BaseItemParserTask:
    queued_urls = set()

    def __init__(self, url, queue, session):
        self.url = url
        self.queue = queue
        self.session = session

    async def run(self):
        response = await self.session.get(self.url)
        response.document = await self.parse_document(response)
        for item in self.items(response):
            self.save(item)

    async def parse_document(self, response):
        text = await response.text()
        return html.fromstring(text)


    def items(self, response):
        raise NotImplementedError()

    def save(self, item):
        raise NotImplementedError()


class BaseSpiderTask:
    parser_class = BaseItemParserTask
    queued_urls = set()

    def __init__(self, url, queue, session, parser_class=None):
        self.url = url
        self.queue = queue
        self.session = session
        if parser_class is not None:
            self.parser_class = None

    async def run(self):
        response = await self.session.get(self.url)
        response.document = await self.parse_document(response)
        self.queue_spiders(response)
        self.queue_items(response)

    async def parse_document(self, response):
        text = await response.text()
        return html.fromstring(text)

    def queue_spiders(self, response):
        for url in self.spider_targets(response):
            if url not in self.queued_urls:
                self.queued_urls.add(url)
                self.queue.put_nowait(self.__class__(url, self.queue, self.session))

    def queue_items(self, response):
        for url in self.item_targets(response):
            if url not in self.queued_urls:
                self.queued_urls.add(url)
                self.queue.put_nowait(self.parser_class(url, self.queue, self.session))

    def spider_targets(self, response):
        raise NotImplementedError

    def item_targets(self, response):
        raise NotImplementedError


class DmozItemParser(BaseItemParserTask):
    def __init__(self, *args, **kwargs):
        self.fd = kwargs.pop('fd')
        super().__init__(*args, **kwargs)

    def items(self, response):
        for item in response.document.xpath('//a[@class="listinglink"]'):
            url = urljoin(response.url, item.attrib['href'])
            yield url

    def save(self, item):
        self.fd.write(str(item) + '\n')


class DmozSpider(BaseSpiderTask):
    parser_class = DmozItemParser

    def __init__(self, *args, **kwargs):
        self.fd = kwargs.pop('fd')
        super().__init__(*args, **kwargs)

    def spider_targets(self, response):
        if False:
            yield ''
        raise StopIteration

    def item_targets(self, response):
        for item in response.document.xpath('//ul[@class="directory dir-col"]/li/a'):
            url = urljoin(response.url, item.attrib['href'])
            yield url


class RemaxItemParser(BaseItemParserTask):
    def __init__(self, *args, **kwargs):
        self.fd = kwargs.pop('fd')
        super().__init__(*args, **kwargs)

    def items(self, response):

        import ipdb; ipdb.set_trace()
        data_dict = {}
        data_items = response.document.xpath('//*[contains(concat(" ", normalize-space(@class), " "), " data-item ")]')
        for data_item in data_items:
            for di in data_item:
                elems = di.xpath('.//*[@title]')
                data_dict[elems[1].attrib['title'].strip().lower()] = elems[2].attrib['title'].strip().lower()

        item = {
            'url': response.url,
            'agent': 'remax',
            'agent_reference': response.document.xpath('//*[@itemprop="productID"]')[0].text.strip(),
            'property_type': 'apartment',
            'transaction_type': 'rent',

            'price': int(response.document.xpath('//*[@itemprop="price"]')[0].text.strip()),  # TODO Decimal
            'address': response.document.xpath('//*[@class="key-address"]')[0].text.strip(),
            'description': response.document.xpath('//*[@itemprop="description"]')[0].text.strip(),
            'lot_size': data_dict['lot size (m2)'],
            'year_built': data_dict['year build'],
            'total_rooms': data_dict['total rooms'],
            'bedrooms': data_dict['bedrooms'],
            'floor_level': data_dict['floor level'],
        }
        yield item

    def save(self, item):
        self.fd.write(str(item) + '\n')


class RemaxSpider(BaseSpiderTask):
    parser_class = RemaxItemParser

    def __init__(self, *args, **kwargs):
        self.fd = kwargs.pop('fd')
        super().__init__(*args, **kwargs)

    def queue_spiders(self, response):
        for url in self.spider_targets(response):
            if url not in self.queued_urls:
                self.queued_urls.add(url)
                self.queue.put_nowait(self.__class__(url, queue=self.queue, session=self.session, fd=self.fd))

    def queue_items(self, response):
        for url in self.item_targets(response):
            if url not in self.queued_urls:
                self.queued_urls.add(url)
                self.queue.put_nowait(self.parser_class(url, queue=self.queue, session=self.session, fd=self.fd))

    async def parse_document(self, response):
        json_data = await response.json()
        return html.document_fromstring(json_data['llContentContainerHtml'])

    def spider_targets(self, response):
        #for item in response.document.xpath('//a[@class="ajax-page-link"]'):
        #    url = urljoin(response.url, item.attrib['href'])
        #    logging.info(url)
        #    self.fd.write(str(url) + '\n')
        #    yield url
        if False:
            yield ''
        raise StopIteration

    def item_targets(self, response):
        for item in response.document.xpath('//div[@class="proplist-address"]/a'):
            url = urljoin(response.url, item.attrib['href']) + '/?Lang=en-US'
            yield url


class Consumer:
    def __init__(self, index, taskqueue):
        self.index = index
        self.stop = False
        self.taskqueue = taskqueue

    async def run(self):
        while True:
            if self.stop:
                break
            try:
                next_task = self.taskqueue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.4)
            else:
                logging.info('%s -- Run task: %s, %s', self.index, next_task.__class__, next_task.url)
                await next_task.run()
                self.taskqueue.task_done()


async def stop(futures, consumers, taskqueue):
    await taskqueue.join()
    for consumer in consumers:
        consumer.stop = True
    await asyncio.wait(futures)


def dmoz_tasks(taskqueue, session):
    tasks = [DmozSpider(url='https://www.dmoz.org/Computers/Programming/Personal_Pages/', queue=taskqueue, session=session)]
    return tasks


def remax_tasks(taskqueue, session, fd):
    base_url = 'http://www.remax.pt/handlers/listinglist.ashx'
    query_data = {
        'mode': 'list',  # view mode (list, gallery, map view)
        'tt': '260',  # Transaction type ? (260 rent?)
        'cr': '2',  # 1: commercial, 2: residential
        'r': '76',  # Region (76: Lisbon)
        'p': '537',  # Province (537 Lisbon)
        'pt': '359',  # Property Type (359 condo/apartment)
        #'rmin': '900',  # Min
        'rmax': '900',  # Maximum
        'cur': 'EUR',  # currency
        'la': 'All',  # ??
        'sb': 'MostRecent', # sort order
        'page': '1',
        'sc': '12',  # selected country (12: portugal)
        #'sid': 'a81a1d1d-ee36-4236-a72e-31343349c574',  # Search id?
    }
    tasks = []
    for page in range(1):
        query_data['page'] = page+1
        tasks.append(RemaxSpider(url=base_url + '?' + urlencode(query_data),
                                 queue=taskqueue,
                                 session=session,
                                 fd=fd
                                 ))
    return tasks


def main():
    fname = 'test'
    fd = open(fname, mode='w', encoding='utf-8')

    taskqueue = asyncio.Queue()

    loop = asyncio.get_event_loop()
    loop.set_debug(DEBUG)
    logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO)
    session = aiohttp.ClientSession(loop=loop)

    logging.info('Initializing taskqueue')
    # tasks = dmoz_tasks(taskqueue, session)
    tasks = remax_tasks(taskqueue, session, fd)
    logging.info('Queueing %s tasks', len(tasks))
    [taskqueue.put_nowait(task) for task in tasks]

    logging.info('Creating %s consumers', CONSUMERS)
    consumers = [Consumer(index=i, taskqueue=taskqueue) for i in range(CONSUMERS)]
    futures = [asyncio.ensure_future(consumer.run()) for consumer in consumers]

    logging.info('Start loop')
    loop.run_until_complete(stop(futures, consumers, taskqueue))

    logging.info('Close loop')
    session.close()
    loop.close()


if __name__ == '__main__':
    main()
