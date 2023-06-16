import tempfile
import os
import argparse
from typing import Union
from urllib.parse import urlparse

from flask import Flask, request, send_file, Response, abort

from clueweb_url_domain_lookup import ClueWebDomainURLLookup

app = Flask(__name__)
tempdir = tempfile.TemporaryDirectory()

def check_mode(req) -> Union[str, None]:
    if 'mode' not in req.args:
        return 'L'

    if req.args['mode'] in ['B', 'A', 'L']:
        return req.args['mode']

    return None

@app.route("/search/<domain>")
def search_domain_endpoint( domain: str) -> Response:
    if not domain.startswith('http:') and not domain.startswith('https:'):
        domain = 'https://' + domain
    
    mode = check_mode(request)
    if mode is None:
        print('Invalid mode value, must be B, A, or L')
        abort(500)

    print(f'Searching {domain}')
    results = lookup.search_domain(domain, mode)
    urlobj = urlparse(domain)
    tempoutput = os.path.join(tempdir.name, f'{urlobj.netloc}.csv')
    lookup.write_results(results, tempoutput)
    return send_file(tempoutput, as_attachment=True, download_name=f'{urlobj.netloc}.csv')

@app.route("/count/<domain>")
def count_domain_endpoint( domain: str) -> str:
    if not domain.startswith('http:') and not domain.startswith('https:'):
        domain = 'https://' + domain

    print(f'Counting hits for {domain}')
    mode = check_mode(request)
    if mode is None:
        print('Invalid mode value, must be B, A, or L')
        abort(500)
    count = lookup.count_domain(domain, mode)
    return f'{count}'

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='Path to ClueWeb SQLite index database', required=True, type=str)
    parser.add_argument('-p', '--port', help='Port number for the webservice', default=5555, type=int)
    args = parser.parse_args()

    lookup = ClueWebDomainURLLookup(args.database)
    app.run(host='0.0.0.0', port=args.port)
