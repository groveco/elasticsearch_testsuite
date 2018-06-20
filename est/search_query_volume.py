import argparse
import csv
from db import DbConnection


class SearchQueryVolume():
    def __init__(self, tag):
        self.tag = tag
        self.db = DbConnection()

    def create_schema(self, tag, clobber=False):
        if clobber:
            self.db.execute('DROP TABLE %s' % self.volume_name)
        self.db.execute("CREATE TABLE %s (id int, count int default 0, query varchar(255), q varchar(255))" % self.volume_name)

    def do_import(self, file, clobber):

        self.create_schema(self.tag, clobber)

        with open(file, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            reader.next()
            id = 1
            for row in reader:
                self.db.execute(
                    "INSERT INTO %s (id, count, query, q) VALUES (:id, :count, :query, :q)" % self.volume_name,
                    {
                        'id': id,
                        'count': row['count'],
                        'query': row['query'],
                        'q': row['q']
                    }
                )
                id += 1

    @property
    def volume_name(self):
        return 'search_queries_volume_%s' % (self.tag,)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--file', required=True, help='CSV query file to import')
    p.add_argument('--tag', required=True, help='Versioning Tag')
    args = p.parse_args()

    volume = SearchQueryVolume(args.tag)
    volume.do_import(args.file, True)
