import argparse
import csv
from db import DbConnection
import xlsxwriter


class SearchResultsReport():

    def __init__(self, queries_tag, results_a_tag, results_b_tag, products_csv_path, sample='top', sample_size=300):
        self.results_a_tag = results_a_tag
        self.results_b_tag = results_b_tag
        self.queries_tag = queries_tag
        self.products_csv_path = products_csv_path
        self.volumes = [self.results_a_tag, self.results_b_tag]
        self.db = DbConnection()
        self.load_products(self.products_csv_path)
        self.sample = self.get_sample_value(sample)
        self.sample_size = sample_size

    def get_sample_value(self, sample_key):
        sample_types = {
            'top': 'count DESC',
            'bottom': 'count ASC',
            'random': 'random()'
        }
        return sample_types.get(sample_key, 'count DESC')

    def load_products(self, products_csv_path):
        self.products = {}
        with open(products_csv_path) as p_fp:
            preader = csv.DictReader(p_fp, delimiter="\t")
            for row in preader:
                self.products[int(row['id'])] = row

    def create(self, filename):
        options = {self.sample: self.sample_size, 'diff': False}

        workbook = xlsxwriter.Workbook(filename)
        ws_search = workbook.add_worksheet('Search Results')

        ws_row = 0
        ws_search.write(0, 0, 'Term')

        for e, engine in enumerate(self.volumes):
            ws_search.write(ws_row, e+1, engine)
          
        ws_row += 2
        data_row = ws_row

        for e, engine in enumerate(self.volumes):
            ws_row = data_row

            if self.sample in options:
                rows = self.db.execute("select id, query, count from search_queries_volume_%s where id in (select id from search_results_volume_%s where processed = 1) order by %s limit %s" % (self.queries_tag, engine, self.sample, self.sample_size))

            for row in rows:
                ws_search.write(ws_row, 0, row['query'] + ' (' + str(row['count']) + ')')
                items = self.db.execute("select item_id, order_id from search_results_items_volume_%s where result_id = %d order by order_id asc" % (engine, row['id']))

                for item in items:
                    if item['item_id'] in self.products:
                        name = self.products[item['item_id']]['name']
                    else:
                        name = 'None'

                    if item['item_id'] in self.products:
                        brand = self.products[item['item_id']]['brand']
                    else:
                        brand = 'None'

                    ws_search.write(ws_row, e+1, '(' + str(item['order_id']) + ') ' + str(name) + ' (' + brand + ')' + ' (' + str(item['item_id']) + ')')
                    ws_row += 1

                max = self._get_max_col_length(self.volumes, row['id'], options['diff'])
                ws_row += max - items.rowcount + 1

        workbook.close()

    def _get_max_col_length(self, volumes, result_id, diff=False):
        max_size = 0

        for v, volume in enumerate(volumes):
            total = self.db.execute("select count from search_results_volume_%s where id = %d" % (volume, result_id)).first()['count']

            if total > max_size:
                max_size = total

        return max_size


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--queries_tag', required=True, help='Search Results Volume Tag to report results')
    p.add_argument('--results_a_tag', required=True, help='Search Results Queries Volume A to report from')
    p.add_argument('--results_b_tag', required=True, help='Search Results Queries Volume B to report from')
    p.add_argument('--products_csv', required=True, help='Products csv file')
    p.add_argument('--filename', required=True, help='Report filename')
    p.add_argument('--sample', required=False, help='Sample top, bottom, or random queries.  Default value is top.')
    p.add_argument('--size', required=False, help='Sample size for queries volumes.  Default value is 300')
    args = p.parse_args()

    report = SearchResultsReport(args.queries_tag, args.results_a_tag, args.results_b_tag, args.products_csv, args.sample, args.size)
    report.create(args.filename)
