import argparse
import db_drivers
from scraper import scrape

parser = argparse.ArgumentParser(description='scrape discourse forums.')
parser.add_argument('output', type=str, help='scrape destination (url or path depending on used driver)')
parser.add_argument('url', type=str, nargs='+', help='urls to be scraped')
parser.add_argument('--database-driver', dest='db_driver', action='store',
                    default="filesystem",
                    help='what driver to use for storing (check db_drivers/)')

args = parser.parse_args()

assert(args.db_driver in db_drivers.drivers)

print(f"database driver: `{args.db_driver}`")

for url in args.url:
    scrape(args.output, args.db_driver, url)