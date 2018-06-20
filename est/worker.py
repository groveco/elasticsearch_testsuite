import argparse
import importlib
import os

from db import DbConnection


def run_worker(tag, ids):
    engine = os.getenv('EST_ENGINE')
    url = os.getenv('EST_ENGINE_PARAMS')
    module = importlib.import_module('engines.' + engine)
    api = getattr(module, 'SearchApi')(url)
    db = DbConnection()

    for id in ids:
        row = db.execute("select q from search_queries_volume_%s where id = %d" % (tag, id)).first()
        results = api.search(row['q'])
        if type(results) != list:
            results = api.search(row['q'])
            if type(results) != list:
                continue

        db.execute("delete from search_results_items_volume_%s where result_id = %d" % (tag, id))
        db.execute("update search_results_volume_%s set processed = 0 where id = %d" % (tag, id))

        order = 1
        for result in results:
            db.execute("insert into search_results_items_volume_%s (result_id, order_id, item_id) values (%d, %d, %d)" % (tag, id, order, result))
            order += 1
        db.execute("update search_results_volume_%s set processed = 1, count = %d where id = %d" % (tag, len(results), id))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--tag', required=True, help='Search Results Volume Tag to store results')
    p.add_argument('--ids', type=int, nargs='*', required=True, help='Search result ids to process')
    args = p.parse_args()

    run_worker(args.tag, args.ids)
