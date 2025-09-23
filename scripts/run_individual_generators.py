# Usage: python scripts/run_individual_generators.py --datasets customers products --scale 0.0005 --out data_raw_individual
"""Generic runner for individual dataset generators.

Example usages (from project root):
  # List available datasets
  python scripts/run_individual_generators.py --list

  # Generate customers only (1% scale)
  python scripts/run_individual_generators.py --datasets customers --scale 0.01

  # Generate multiple datasets
  python scripts/run_individual_generators.py --datasets customers products stores --scale 0.02

  # Generate all supported datasets at tiny sample scale
  python scripts/run_individual_generators.py --all --scale 0.001

  # Custom output dir & seed
  python scripts/run_individual_generators.py --datasets suppliers --out data_raw --seed 123

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

# Dataset registry: dataset_name -> { 'schema_attr': str, 'module': str, 'func': str, 'ext': str, 'dependencies': list, 'partitioned': bool }
DATASET_REGISTRY = {
    'customers':     {'schema_attr': 'customers_schema',     'module': 'scripts.generators.customers',     'func': 'generate_customers_data',     'ext': 'csv', 'dependencies': [], 'partitioned': False},
    'products':      {'schema_attr': 'products_schema',      'module': 'scripts.generators.products',      'func': 'generate_products_data',      'ext': 'csv', 'dependencies': [], 'partitioned': False},
    'stores':        {'schema_attr': 'stores_schema',        'module': 'scripts.generators.stores',        'func': 'generate_stores_data',        'ext': 'csv', 'dependencies': [], 'partitioned': False},
    'suppliers':     {'schema_attr': 'suppliers_schema',     'module': 'scripts.generators.suppliers',     'func': 'generate_suppliers_data',     'ext': 'csv', 'dependencies': [], 'partitioned': False},
    'exchange_rates': {'schema_attr': 'exchange_rates_schema', 'module': 'scripts.generators.exchange_rates', 'func': 'generate_exchange_rates_data', 'ext': 'xlsx', 'dependencies': [], 'partitioned': False},
    'shipments':     {'schema_attr': 'shipments_schema',     'module': 'scripts.generators.shipments',     'func': 'generate_shipments_data',     'ext': 'parquet', 'dependencies': [], 'partitioned': False},
    'orders_header': {'schema_attr': 'orders_header_schema', 'module': 'scripts.generators.orders_header', 'func': 'generate_orders_header_data', 'ext': 'csv', 'dependencies': ['customers', 'stores'], 'partitioned': True},
    'orders_lines':  {'schema_attr': 'orders_lines_schema',  'module': 'scripts.generators.orders_lines',  'func': 'generate_orders_lines_data',  'ext': 'csv', 'dependencies': ['products', 'orders_header'], 'partitioned': True},
    'events':        {'schema_attr': 'events_schema',        'module': 'scripts.generators.events',        'func': 'generate_events_data',        'ext': 'jsonl', 'dependencies': ['customers'], 'partitioned': True},
    'sensors':       {'schema_attr': 'sensors_schema',       'module': 'scripts.generators.sensors',       'func': 'generate_sensors_data',       'ext': 'csv', 'dependencies': ['stores'], 'partitioned': True},
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
    return func, schema, info


def resolve_dependencies(selected_datasets):
    """Resolve dataset dependencies and return execution order."""
    resolved = []
    seen = set()
    
    def add_with_deps(dataset):
        if dataset in seen:
            return
        seen.add(dataset)
        
        # Add dependencies first
        deps = DATASET_REGISTRY[dataset]['dependencies']
        for dep in deps:
            if dep in DATASET_REGISTRY:
                add_with_deps(dep)
        
        # Add the dataset itself
        if dataset not in resolved:
            resolved.append(dataset)
    
    for dataset in selected_datasets:
        add_with_deps(dataset)
    
    return resolved


def calculate_row_counts(generated_datasets, scale):
    """Calculate row counts for datasets that have been generated."""
    from utils.constants import TARGET_ROWS
    from utils.data_utils import apply_scale_to_targets
    
    counts = {}
    for dataset in generated_datasets:
        if dataset in TARGET_ROWS:
            counts[dataset] = apply_scale_to_targets(TARGET_ROWS[dataset], scale)
    return counts


def main():
    args = parse_args()
    random.seed(args.seed)

    if args.list:
        print("Available datasets:")
        for name in sorted(DATASET_REGISTRY.keys()):
            deps = DATASET_REGISTRY[name]['dependencies']
            dep_str = f" (requires: {', '.join(deps)})" if deps else ""
            print(f"  - {name}{dep_str}")
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

    # Resolve dependencies and get execution order
    execution_order = resolve_dependencies(selected)
    print(f"[info] Execution order: {' -> '.join(execution_order)}")

    out_root = pathlib.Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    summary = []
    generated_counts = {}
    
    for ds in execution_order:
        func, schema, info = resolve_generator(ds)
        
        # Handle partitioned vs single file output
        if info['partitioned']:
            out_path = out_root  # Base directory for partitioned output
            print(f"[info] Generating {ds} -> {out_path}/<partitions> (scale={args.scale})")
        else:
            out_path = out_root / f"{ds}.{info['ext']}"
            print(f"[info] Generating {ds} -> {out_path} (scale={args.scale})")
        
        # Handle different function signatures
        if ds == 'orders_header':
            # Special case: orders_header needs customer and store counts
            row_counts = calculate_row_counts(['customers', 'stores'], args.scale)
            num_customers = generated_counts.get('customers', row_counts.get('customers', 0))
            num_stores = generated_counts.get('stores', row_counts.get('stores', 0))
            result = func(schema, args.scale, out_path, num_customers, num_stores)
            # Handle tuple return from orders_header
            rows = result[0] if isinstance(result, tuple) else result
        elif ds == 'orders_lines':
            # Special case: orders_lines requires complex parameters from orders_header generation
            print(f"[warning] {ds} requires complex dependencies from orders_header generation.")
            print(f"[warning] Use generate_data.py for full integrated generation of orders data.")
            rows = 0  # Skip for now
        else:
            # Standard generator signature
            rows = func(schema, args.scale, out_path)
        
        generated_counts[ds] = rows
        summary.append((ds, rows, out_path))
        print(f"[ok] {ds}: {rows} rows")

    print("\nSummary:")
    for ds, rows, path in summary:
        path_str = f"{path}/<partitions>" if DATASET_REGISTRY[ds]['partitioned'] else str(path)
        print(f"  {ds:<15} {rows:>8} rows -> {path_str}")


if __name__ == '__main__':
    main()
