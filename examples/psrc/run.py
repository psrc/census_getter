# import argparse
# import sys
# from pypyr import pipelinerunner


# def add_run_args(parser):
#     parser.add_argument(
#         "-w",
#         "--working_dir",
#         type=str,
#         metavar="PATH",
#         help="path to working dir that contains settings.yaml",
#     )

# def run(args):
#     working_dir = args.working_dir
#     pipelinerunner.run(f'{working_dir}/settings')

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     add_run_args(parser)
#     args = parser.parse_args()
#     sys.exit(run(args))

from pypyr import pipelinerunner

pipelinerunner.run('configs/settings')