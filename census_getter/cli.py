import argparse
import sys

from census_getter.run import add_run_args, run


def build_parser():
    parser = argparse.ArgumentParser(prog="census_getter")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="run the census_getter pipeline",
    )
    add_run_args(run_parser)
    run_parser.set_defaults(func=run)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
