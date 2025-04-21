import scrapy

class MovieItem(scrapy.Item):
    """Represents a movie item."""
    title = scrapy.Field()
    url = scrapy.Field()
    quotes = scrapy.Field()

class QuoteItem(scrapy.Item):
    """Represents a quote item."""
    text = scrapy.Field()
    movie_title = scrapy.Field()