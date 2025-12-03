# Replayability

It's reasonable to accommodate:
- Whole backfills
- Backfills from a given date
- Replay of one or more specific records

Within this proposed architecture, that translates to accepting configurable parameters for:
1. Full backfill
2. Reprocess from a given date
3. Rehydrate (reingest) one or more specific rows

Reingestion and renormalization are manually triggered, following a cadence that aligns with system schedules.

## How to run

### Pseudocode

- Validate input mode  
- Conditionally execute reingestion logic:
  - If full, invoke a full ingestion run
  - If from date, invoke ingestion from the given date
  - If targeted, invoke ingestion of one or more specific IDs
- Once records are reingested, they are renormalized and reoutboxed, after which the connector republishes automatically.

This means the replay code should only handle inputs and invoke jobs, while ingestion logic supports all three modes. The exact definition of a “full” or “partial” run may vary per source system.

This approach can be implemented in any serverless environment (AWS Lambda, MWAA, Glue).

### Vanilla Python Example

If reingesting is enough:

```
import argparse

parser = argparse.ArgumentParser()

parser.add_argument(
    "--mode",
    type=str,
    choices=["full", "since", "targeted"],
    default="full"
)
parser.add_argument(
    "--date",
    type=str, 
    help="Start date in YYYY-MM-DD format (required when --mode=since)"
)
parser.add_argument(
    "--ids",
    type=lambda s: [x.strip() for x in s.split(",") if x.strip()],
    help="Comma-separated IDs (required when --mode=targeted)"
)

args = parser.parse_args()

if args.mode == "since" and not args.date:
    parser.error("--date is required when --mode=since")

if args.mode == "targeted" and not args.ids:
    parser.error("--ids is required when --mode=targeted")

print(args.mode, args.date, args.ids)

--

python rest_api.py --mode full

python rest_api.py --mode since --date 2025-09-01

python rest_api.py --mode targeted --ids 1,2,3
```

**Bulk targeted**

```
IDS=$(sed '1d' <INSERT>.csv | tr '\n' ',' | sed 's/,$//')

python rest_api.py --mode targeted --ids "$IDS"
```

If immediate renormalization is required:

```
python 2.normalization/normalize.py 

python 2.normalization/normalize.py --mode full

python 2.normalization/normalize.py --mode targeted --ids 1,2,3

python 2.normalization/normalize.py --mode since --date 2025-12-10
```

Where excluding `mode` executes BAU logic, ie, "anything created in the last 24 hours", etc.