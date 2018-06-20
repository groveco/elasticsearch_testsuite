import json
import requests

class SearchApi:
    def __init__(self, url):
        self.url = url

    def search(self, q):
        try:
            query = self.url % str(q)
            resp = requests.get(self.url % q)
            data = json.loads(resp.text)
            results = []
            for r in data['results']:
                results.append(r['id'])
            return results
        except Exception as e:
            print(e)
            return False
        return []
