# Elasticsearch Test Suite

# Environment

The following environment settings are used:

* EST_DATABASE - Database name
* EST_HOST - Database host
* EST_PORT - Database port
* EST_LOGIN - Database login
* EST_PASSWORD - Database password
* EST_ENGINE - Search engine (eg. grove_product_search)
* EST_ENGINE_PARAMS - Search engine parameter (eg 'https://gext.co/api/pantry/products/search/?%s&page=1&page_size=1000&show_unavailable=true')

# Example Run

### Import search queries from a csv into the queries volume 'test'
python search_query_volume.py  --file queries.csv  --tag test

### Initialize the results volume 'test' from the search queries volume 'test'
python search_results_volume.py  --queries_tag test --results_tag test

### Run searches from the queries volume 'test' and store the results in the results volume 'test'
python controller.py --queries_tag test --results_tag test

### Generate a search results excel report by comparing two search volumes.
python search_results_report.py --queries_tag test --results_a_tag results1 --results_b_tag results2 --products_csv products.csv --filename report.xls
