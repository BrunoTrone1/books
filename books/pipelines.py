# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import couchdb
import os
   
class CouchDBPipeline:
    def __init__(self, couchdb_uri, couchdb_db):
        self.couchdb_uri = couchdb_uri
        self.couchdb_db =couchdb_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            couchdb_uri = crawler.settings.get('COUCHDB_URI'),
            couchdb_db = crawler.settings.get('COUCHDB_DB')
        )

    def open_spider (self, spider):
        self.server = couchdb.Server(self.couchdb_uri)
        try:
            self.db = self.server.create(self.couchdb_db)
        except couchdb.http.PreconditionFailed:
            self.db = self.server[self.couchdb_db] 

    def close_spider(self, spider):
        self.server.logout()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        doc = dict(adapter.asdict())
        self.db.save(doc)
        return item