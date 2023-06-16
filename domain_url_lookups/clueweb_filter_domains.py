import argparse
import os

class ClueWebDomainFiltering:

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

    def run(self):
        all_domains = {}

        files = [os.path.join(self.args.files, f) for f in os.listdir(self.args.files) if f.endswith('.domains')]
                
        for i, f in enumerate(files):
            with open(f, 'r') as fp:
                print(f'> File {i+1}/{len(files)} {f}')
                for row in fp.readlines():
                    domain, count = row.strip().split('\t')
                    if domain in all_domains:
                        all_domains[domain] += int(count)
                    else:
                        all_domains[domain] = int(count)
                print(f'> Finished {f}')

        print(f'> Collected {len(all_domains)} unique domains')

        c = 0
        with open(self.args.output, 'w') as f:
            for domain, count in all_domains.items():
                if count >= self.args.count:
                    f.write(f'{domain},{count}\n')
                    c += 1

        print(f'> Wrote {c} domains with 500+ records')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--files', help='Path to folder containing .domains files', required=True, type=str)
    parser.add_argument('-c', '--count', help='Only include domains with at least this many records', required=True, type=int)
    parser.add_argument('-o', '--output', help='output filename', required=True, type=str)
    args = parser.parse_args()
    ClueWebDomainFiltering(args).run()
