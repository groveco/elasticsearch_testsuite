import argparse
import subprocess

from db import DbConnection


def run_controller(search_results_tag):
    db = DbConnection()

    total = db.execute("select count(*) as total from search_results_volume_%s" % search_results_tag).first()['total']

    batches = []

    for i in xrange(1, total + 1, 100):
        batch = []
        for j in xrange(i, i + 100):
            if j <= total:
                batch.append(str(j))
        batches.append(batch)


    while len(batches) > 0:
        procs = []

        for allowed in xrange(1, 8):
            if len(batches) > 0:
                batch = batches.pop()
                p = subprocess.Popen(["python", "worker.py", "--tag", search_results_tag, "--ids"] + batch)
                procs.append(p)

        for p in procs:
            p.wait()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--search_results_tag', required=True, help='Search Results Volume Tag')
    args = p.parse_args()

    run_controller(args.search_results_tag)