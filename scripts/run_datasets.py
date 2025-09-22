# Usage: python scripts/run_datasets.py --datasets customers products --scale 0.0005 --out data_raw_individual
"""Generic runner for individual dataset generators.

Example usages (from project root):
  # List available datasets
  python scripts/run_datasets.py --list

  # Generate customers only (1% scale)
  python scripts/run_datasets.py --datasets customers --scale 0.01

  # Generate multiple datasets
  python scripts/run_datasets.py --datasets customers products stores --scale 0.02

  # Generate all supported datasets at tiny sample scale
  python scripts/run_datasets.py --all --scale 0.001

  # Custom output dir & seed
  python scripts/run_datasets.py --datasets suppliers --out data_raw --seed 123

Notes:
  * Each generator returns number of rows written.
  * Output file naming convention: <dataset>.csv
  * Extend DATASET_REGISTRY to add new generators.
"""
from __future__ import annotations
import argparse
import importlib
import pathlib
import random
import sys
from typing import Callable, Dict, Any

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import schemas once
from schemas import schemas as schemas_mod  # type: ignore

# Dataset registry: dataset_name -> { 'schema_attr': str, 'module': str, 'func': str, 'ext': str }
DATASET_REGISTRY = {
    'customers':  {'schema_attr': 'customers_schema',  'module': 'scripts.generators.customers',  'func': 'generate_customers_data',  'ext': 'csv'},
    'products':   {'schema_attr': 'products_schema',   'module': 'scripts.generators.products',   'func': 'generate_products_data',   'ext': 'csv'},
    'stores':     {'schema_attr': 'stores_schema',     'module': 'scripts.generators.stores',     'func': 'generate_stores_data',     'ext': 'csv'},
    'suppliers':  {'schema_attr': 'suppliers_schema',  'module': 'scripts.generators.suppliers',  'func': 'generate_suppliers_data',  'ext': 'csv'},
}


def parse_args():
    ap = argparse.ArgumentParser(
        description="Generate one or more synthetic datasets (customers/products/stores/suppliers)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument('--datasets', nargs='+', help='Specific dataset names to generate')
    ap.add_argument('--all', action='store_true', help='Generate all available datasets')
    ap.add_argument('--scale', type=float, default=0.01, help='Scaling factor relative to TARGET_ROWS')
    ap.add_argument('--out', type=str, default='data_raw', help='Output directory root')
    ap.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    ap.add_argument('--list', action='store_true', help='List available datasets and exit')
    return ap.parse_args()


def resolve_generator(dataset: str):
    info = DATASET_REGISTRY[dataset]
    module = importlib.import_module(info['module'])
    func = getattr(module, info['func'])
    schema = getattr(schemas_mod, info['schema_attr'])
    return func, schema, info['ext']


def main():
    args = parse_args()
    random.seed(args.seed)

    if args.list:
        print("Available datasets:")
        for name in sorted(DATASET_REGISTRY.keys()):
            print(f"  - {name}")
        return

    if args.all:
        selected = list(DATASET_REGISTRY.keys())
    else:
        if not args.datasets:
            raise SystemExit("Provide --datasets <names> or use --all or --list")
        unknown = [d for d in args.datasets if d not in DATASET_REGISTRY]
        if unknown:
            raise SystemExit(f"Unknown dataset(s): {', '.join(unknown)}. Use --list to see options.")
        selected = args.datasets

    out_root = pathlib.Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    summary = []
    for ds in selected:
        func, schema, ext = resolve_generator(ds)
        out_path = out_root / f"{ds}.{ext}"
        print(f"[info] Generating {ds} -> {out_path} (scale={args.scale})")
        rows = func(schema, args.scale, out_path)  # type: ignore[arg-type]
        summary.append((ds, rows, out_path))
        print(f"[ok] {ds}: {rows} rows")

    print("\nSummary:")
    for ds, rows, path in summary:
        print(f"  {ds:<10} {rows:>8} rows -> {path}")


if __name__ == '__main__':
    main()



