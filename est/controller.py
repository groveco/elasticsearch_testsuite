import argparse
import subprocess
from six.moves import range

from db import DbConnection


def run_controller(search_queries_tag, search_results_tag, sample, size):
    db = DbConnection()

    data = db.execute("select id as ids from search_queries_volume_%s order by %s limit %s" % (search_queries_tag, get_sample_value(sample), size))
    ids = [str(item[0]) for item in data.fetchall()] if data else None
    batches = create_batches(ids)

    while len(batches) > 0:
        procs = []

        for allowed in range(1, 8):
            if len(batches) > 0:
                batch = batches.pop()
                p = subprocess.Popen(["python", "worker.py", "--results_tag", search_results_tag, "--queries_tag", search_queries_tag, "--ids"] + batch)
                procs.append(p)

        for p in procs:
            p.wait()


def create_batches(id_list):
    return [id_list[i:i+100] for i in range(0, len(id_list), 100)] 


def get_sample_value(sample_key):
    sample_types = {
        'top': 'count DESC',
        'bottom': 'count ASC',
        'random': 'random()'
    }
    return sample_types.get(sample_key, "count DESC")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--queries_tag', required=True, help='Search Results Volume Tag to store results')
    p.add_argument('--results_tag', required=True, help='Search Results Queries Volume to search from')
    p.add_argument('--sample', required=False, default='top', help='Sample type for query volume. Accepted values are top (default), bottom, and random')
    p.add_argument('--size', required=False, default="all", type=str, help='Sample size for queries to run.  Runs all by default')
  
    args = p.parse_args()

    run_controller(args.queries_tag, args.results_tag, args.sample, args.size)