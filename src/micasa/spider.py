

class StackOverflowSpider(scrapy.Spider):
    name = 'casa'
    start_urls = ['http://homelovers.pt/index.php/component/jak2filter/?Itemid=114&isc=1&category_id=2&xf_7_range=0|1200&ordering=order']

    def parse(self, response):
        for href in response.css('.question-summary h3 a::attr(href)'):
            full_url = response.urljoin(href.extract())
            yield scrapy.Request(full_url, callback=self.parse_question)

    def parse_question(self, response):
        yield {
            'title': response.css('h1 a::text').extract()[0],
            'votes': response.css('.question .vote-count-post::text').extract()[0],
            'body': response.css('.question .post-text').extract()[0],
            'tags': response.css('.question .post-tag::text').extract(),
            'link': response.url,
        }




class MicasaSpider():
    start_urls = []

    def parse(self):
        pass



    def parse_detail(self):
        pass

