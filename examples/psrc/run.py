import argparse
import sys
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
    configs_dir = args.configs_dir
    print(f"Running census_getter pipeline with configs in: {configs_dir}")
    pipelinerunner.run(f'{configs_dir}/settings', dict_in={'configs_dir': configs_dir})

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    add_run_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))