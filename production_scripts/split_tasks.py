#!/usr/bin/env python3

import argparse
import glob
import json
import os


def create_parser():
    """Set up list of command-line arguments to split_tasks.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--N', type=int, default=100, help="""
        Split task list(s) into lists of N elements (or fewer) each (default:
        %(default)s).""")
    parser.add_argument('--tasklist', required=True, help="""
        Single tasklist file (json) or directory of similar tasklist files.""")
    parser.add_argument('--output_dir', default='.', help="""
        Directory to which resulting task lists are to be saved (default:
        '%(default)s').""")

    return parser


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    if os.path.isfile(args.tasklist):
        all_tasklists = [args.tasklist]
    else:
        all_tasklists = [
            os.path.join(args.tasklist,file)
            for file in os.listdir(args.tasklist)
            if os.path.splitext(file)[1] == '.json']

    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)

    for tasklist in all_tasklists:
        n = 0
        t = json.load(open(tasklist))
        t_basename,t_ext = os.path.splitext(os.path.basename(tasklist))
        while n*args.N < len(t):
            json.dump(
                t[n*args.N:(n+1)*args.N],
                open(os.path.join(args.output_dir,f'{t_basename}_{n}{t_ext}'),'w'),
                indent=4)
            n = n+1


if __name__ == '__main__':
    main()
