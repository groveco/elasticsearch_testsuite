from db import DbConnection


class SearchResultsVolume():
    def __init__(self, tag):
        self.tag = tag
        self.db = DbConnection()

    def create_schema(self, clobber=False):
        if clobber:
            try:
                self.db.execute('DROP TABLE %s' % self.volume_name)
                self.db.execute('DROP TABLE %s' % self.volume_items_name)
            except:
                pass

        self.db.execute("CREATE TABLE %s (id int, count int default 0, processed int default 0)" % self.volume_name)
        self.db.execute("CREATE TABLE %s (id serial, result_id int, order_id int, item_id int, result text, UNIQUE (result_id, item_id))" % self.volume_items_name)

    @property
    def volume_name(self):
        return 'search_results_volume_%s' % (self.tag,)

    @property
    def volume_items_name(self):
        return 'search_results_items_volume_%s' % (self.tag,)
