import argparse
import sys
from pathlib import Path
from pypyr import pipelinerunner


def add_run_args(parser):
    parser.add_argument(
        "-c",
        "--configs_dir",
        type=str,
        metavar="PATH",
        default="configs",
        help="path to configs dir that contains settings.yaml (default: configs)",
    )

def run(args):
    configs_dir = str(Path(args.configs_dir).resolve())
    print(f"Running control-totals pipeline with configs in: {configs_dir}")
    pipelinerunner.run(f'{configs_dir}/settings', dict_in={'configs_dir': configs_dir})


def build_parser():
    parser = argparse.ArgumentParser()
    add_run_args(parser)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return run(args)

if __name__ == '__main__':
    sys.exit(main())

