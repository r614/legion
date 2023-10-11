import sys
from legion.migrate import run_migration
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, required=True, help="PostgreSQL URL")
args = parser.parse_args()


if run_migration(args.url):
    print("migration completed successfully")
else:
    print("Failed to run migration")
    sys.exit(1)
