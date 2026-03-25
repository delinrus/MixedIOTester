from __future__ import annotations

import argparse
import json
import sys

from config import ConfigError, config_to_dict, load_config
from report import print_summary, write_csv_report, write_json_report
from runner import Runner


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="MixedIOTester: mixed RR/RW/SR/SW load tool")
    p.add_argument("--config", required=True, help="Path to YAML/JSON config")
    p.add_argument(
        "--override",
        action="append",
        default=[],
        help="Override key=value, supports dotted paths (repeatable)",
    )
    p.add_argument("--dry-run", action="store_true", help="Validate and print effective config only")
    p.add_argument("--print-effective-config", action="store_true", help="Print effective config JSON")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        cfg = load_config(args.config, overrides=args.override)
    except (ConfigError, ValueError, KeyError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if args.print_effective_config or args.dry_run:
        print(json.dumps(config_to_dict(cfg), indent=2))
    if args.dry_run:
        # rough memory estimate: num_threads * max write block size buffer
        max_bs = max(op.block_size for op in cfg.operations.values() if op.enabled and op.share > 0)
        estimate = cfg.test.num_threads * max_bs
        print(f"estimated_buffer_bytes={estimate}")
        return 0

    runner = Runner(cfg)
    summary = runner.run()
    if cfg.output.print_summary:
        print_summary(cfg, summary)
    if cfg.output.save_json:
        write_json_report(cfg, summary, cfg.output.json_path)
    if cfg.output.save_csv:
        write_csv_report(summary, cfg.output.csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
