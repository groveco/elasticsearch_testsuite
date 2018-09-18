import argparse
import csv
from db import DbConnection
import xlsxwriter
from collections import namedtuple


class SearchResultsReport():
    def __init__(
        self, queries_tag, results_a_tag, results_b_tag, search_items_csv_path,
        filename, sample, sample_size, diff
    ):
        self.results_a_tag = results_a_tag
        self.results_b_tag = results_b_tag
        self.queries_tag = queries_tag
        self.search_items_csv_path = search_items_csv_path
        self.volumes = [self.results_a_tag, self.results_b_tag]
        self.db = DbConnection()
        self.load_search_items(self.search_items_csv_path)
        self.sample = self.get_sample_value(sample)
        self.sample_size = sample_size if sample_size else '300'
        self.all_results = not diff

    def get_sample_value(self, sample_key):
        sample_types = {'top': 'count DESC', 'bottom': 'count ASC'}
        return sample_types.get(sample_key, 'count DESC')

    def load_search_items(self, search_items_csv_path):
        self.search_items = {}
        with open(search_items_csv_path) as p_fp:
            preader = csv.DictReader(p_fp, delimiter="\t")
            for row in preader:
                self.search_items[int(row['id'])] = row

    def create(self, filename):
        workbook = xlsxwriter.Workbook(filename)
        ws_search = workbook.add_worksheet('Search Results')

        ws_row = 0
        ws_search.write(0, 0, 'Term')
        ws_search.write(ws_row, 1, self.volumes[0])
        ws_search.write(ws_row, 2, self.volumes[1])

        ws_row += 2

        search_ids, all_results = self._get_search_results(
            self.queries_tag, self.volumes, self.sample, self.sample_size
        )
        base_results = all_results[0]
        comparison_results = all_results[1]

        for sid in search_ids:
            base_set = (filter(lambda b: b.search_id == sid, base_results))
            comparison_set = (
                filter(lambda c: c.search_id == sid, comparison_results)
            )

            if all_results or base_set != comparison_set:
                if base_set:
                    ws_search.write(
                        ws_row, 0,
                        str(base_set[0].query) + ' (' + str(base_set[0].count) +
                        ')'
                    )
                base_count = len(base_set)
                comp_count = len(comparison_set)
                max_count = base_count if base_count >= comp_count else comp_count

                for i in range(0, max_count):
                    if base_count > 0:
                        base = base_set.pop(0)
                        base_item = self.search_items[
                            base.item_id] if self.search_items[base.item_id
                                                              ] else ''
                        ws_search.write(
                            ws_row, 1, '(' + str(base.order) + ') ' +
                            base_item['name'] + ' (' + base_item['description']
                            + ')' + ' (' + str(base.item_id) + ')'
                        )
                        base_count = base_count - 1

                    if comp_count > 0:
                        comp = comparison_set.pop(0)
                        comp_item = self.search_items[
                            comp.item_id] if self.search_items[comp.item_id
                                                              ] else ''
                        ws_search.write(
                            ws_row, 2, '(' + str(comp.order) + ') ' +
                            comp_item['name'] + ' (' + comp_item['description']
                            + ')' + ' (' + str(comp.item_id) + ')'
                        )
                        comp_count = comp_count - 1

                    ws_row += 1
                    max_count = 0
        workbook.close()

    def _get_search_results(self, queries_tag, volumes, sample, size):
        results = []
        searches = []
        for v, volume in enumerate(self.volumes):
            result_list = []
            SearchResult = namedtuple(
                'Result', ['search_id', 'query', 'count', 'item_id', 'order']
            )

            rows = self.db.execute(
                "select s.id as search_id, query, s.count, ri.item_id as item_id, ri.order_id \
            from search_queries_volume_{0} as s join search_results_volume_{1} as r on s.id = r.id \
            left join search_results_items_volume_{1} as ri on r.id = ri.result_id \
            where processed = 1 and s.id in (select ss.id from search_queries_volume_{0} ss order by {2} limit {3})\
            order by s.id desc, order_id;".format(
                    queries_tag, volume, sample, size
                )
            ).fetchall()

            for row in map(SearchResult._make, rows):
                if row.search_id not in searches:
                    searches.append(row.search_id)
                result_list.append(row)
            results.append(result_list)
        return searches, results

    def _get_max_col_length(self, volumes, result_id, diff=False):
        max_size = 0

        for v, volume in enumerate(volumes):
            total = self.db.execute(
                "select count from search_results_volume_%s where id = %d" %
                (volume, result_id)
            ).first()['count']

            if total > max_size:
                max_size = total

        return max_size


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        '--queries_tag',
        required=True,
        help='Search Results Volume Tag to report results'
    )
    p.add_argument(
        '--results_a_tag',
        required=True,
        help='Search Results Queries Volume A to report from'
    )
    p.add_argument(
        '--results_b_tag',
        required=True,
        help='Search Results Queries Volume B to report from'
    )
    p.add_argument(
        '--search_items_csv', required=True, help='search_items csv file'
    )
    p.add_argument('--filename', required=True, help='Report filename')
    p.add_argument(
        '--sample',
        required=False,
        help='Sample top, bottom, or random queries.  Default value is top.'
    )
    p.add_argument(
        '--size',
        required=False,
        nargs='?',
        type=str,
        help='Sample size for queries volumes.  Default value is 300'
    )
    p.add_argument(
        '--diff',
        required=False,
        type=bool,
        help=
        'Only include search terms with changes in report. Default value is False.'
    )
    args = p.parse_args()

    report = SearchResultsReport(
        args.queries_tag, args.results_a_tag, args.results_b_tag,
        args.search_items_csv, args.filename, args.sample, args.size, args.diff
    )
    report.create(args.filename)
