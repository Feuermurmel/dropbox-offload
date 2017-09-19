#! /usr/bin/env python3

import argparse
import collections
import math
import os
import re
import shutil
import sys


class UserError(Exception):
    def __init__(self, msg, *args):
        self.message = msg.format(*args)


def log(msg, *args):
    print(msg.format(*args), file=sys.stderr)


def numeric_sort_key(str):
    return re.sub('[0-9]+', lambda x: '%s0%s' % ('1' * len(x.group()), x.group()), str)


def is_subdir_of(inner, outer):
    return os.path.commonprefix([inner, outer]) == outer


def iter_child_dirs(dir):
    for i in os.listdir(dir):
        if not i.startswith('.') and os.path.isdir(os.path.join(dir, i)):
            yield i


def iter_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        for list in dirnames, filenames:
            list[:] = (i for i in list if not i.startswith('.'))

        for i in filenames:
            path = os.path.join(dirpath, i)

            if os.path.isfile(path):
                yield os.path.relpath(path, root)


def get_size(path):
    return os.stat(path).st_size


def remove_empty_parents(path, root):
    parent = os.path.dirname(path)

    if is_subdir_of(parent, root) and all(i.startswith('.') and not os.path.isdir(os.path.join(path, i)) for i in os.listdir(parent)):
        remove(parent, root)


def remove(path, remove_parents_up_to):
    log_path = os.path.relpath(path, remove_parents_up_to)

    if os.path.isdir(path):
        log('Removing directory: {}', log_path)

        shutil.rmtree(path)
    else:
        log('Removing: {}', log_path)

        os.unlink(path)

    if remove_parents_up_to is not None:
        remove_empty_parents(path, remove_parents_up_to)


def rename(source, target):
    parent = os.path.dirname(target)

    if not os.path.exists(parent):
        os.makedirs(parent)

    os.rename(source, target)


def size_arg(arg):
    suffixes = 'kmgtpezy'
    match = re.match('(?P<number>[0-9.]+)(?P<suffix>[A-Za-z]?)$', arg)

    if not match:
        raise ValueError()

    suffix = match.group('suffix')

    if suffix:
        if suffix.isupper():
            factor_base = 1024
        else:
            factor_base = 1000

        pos = suffixes.find(suffix.lower())

        if pos < 0:
            raise ValueError()

        factor = factor_base ** (pos + 1)
    else:
        factor = 1

    return int(float(match.group('number')) * factor)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-n',
        '--per-directory-limit',
        type=int,
        default=math.inf,
        help='Maximum number of files to put into the active dir per '
             'top-level directory. By default no limit applies, unless no '
             'limit is set for both --global-limit and --size-limit, in which '
             'case is defaults to 3.')

    parser.add_argument(
        '-N',
        '--global-limit',
        type=int,
        default=math.inf,
        help='Maximum number of files to put into the active dir. By default '
             'no limit applies.')

    parser.add_argument(
        '-m',
        '--global-minimum',
        type=int,
        default=1,
        help='Minimum number of files to put into the active dir. This '
             'defaults to one.')

    parser.add_argument(
        '-s',
        '--size-limit',
        type=size_arg,
        default=math.inf,
        help='Maximum number combined file size to put into the active dir as '
             'number of bytes. You can use suffixes k, m ... y for powers of '
             '1000 and K, M ... for powers of 1024. By default no limit '
             'applies.')

    parser.add_argument(
        'active_dir',
        help='The directory into which the active files should be put.')

    parser.add_argument(
        'offload_dir',
        help='THe directory into which the offloaded files should be put.')

    args = parser.parse_args()

    if is_subdir_of(args.active_dir, args.offload_dir) or is_subdir_of(args.offload_dir, args.active_dir):
        raise UserError(
            'The active dir and offload dirs may not contain each other or be '
            'the same directory.')

    if args.per_directory_limit == math.inf and args.global_limit == math.inf and args.size_limit == math.inf:
        args.per_directory_limit = 3

    return args


def collect_files_with_sizes(active_dir, offload_dir):
    size_by_file_by_dir = collections.defaultdict(dict)
    for i in [active_dir, offload_dir]:
        for j in iter_child_dirs(i):
            for k in iter_files(os.path.join(i, j)):
                size_by_file_by_dir[j][k] = get_size(os.path.join(i, j, k))
    return size_by_file_by_dir


def select_files(size_by_file_by_dir, per_directory_limit, global_limit, size_limit, global_minimum):
    """
    Return a list of tuples (activate, dir, file) for each file where activate
    is a boolean telling whether the file should be activated instead of
    offloaded, dir is the top-level-directory name in which the file is and
    file is the path relative to the top-level directory.
    """
    def key(x):
        dir, (index, file) = x

        # Prefer files sorted before other files in the same directory,
        # then files in directories with more files. Then sort by name and
        # use the directory name as a last resort ordering criterion.
        return index, -len(size_by_file_by_dir[dir]), numeric_sort_key(file), dir

    # Tuples (dir, (index, file)) sorted by index, file and then dir
    ordered_files = sorted(
        (
            (dir, i)
            for dir, size_by_file in size_by_file_by_dir.items()
            for i in enumerate(
                sorted(size_by_file.keys(), key=numeric_sort_key))),
        key=key)

    def iter_files():
        num_files_by_directory = collections.defaultdict(lambda: 0)
        num_files = 0
        num_bytes = 0

        # Used to prevent that some files from a top-level directory are
        # included while preceding files from the same directory are not.
        capped_directories = set()

        for dir, (_, file) in ordered_files:
            size = size_by_file_by_dir[dir][file]
            activate = (
                num_files < global_minimum
                or (
                    num_files_by_directory[dir] < per_directory_limit
                    and num_files < global_limit
                    and size <= size_limit - num_bytes)) \
                       and dir not in capped_directories

            yield activate, dir, file

            if activate:
                num_files_by_directory[dir] += 1
                num_files += 1
                num_bytes += size
            else:
                capped_directories.add(dir)

    return list(iter_files())


def main(active_dir, offload_dir, per_directory_limit, global_limit, size_limit, global_minimum):
    size_by_file_by_dir = collect_files_with_sizes(active_dir, offload_dir)

    files = select_files(
        size_by_file_by_dir,
        per_directory_limit,
        global_limit,
        size_limit,
        global_minimum)

    for activate, dir, file in files:
        active_path = os.path.join(active_dir, dir, file)
        offload_path = os.path.join(offload_dir, dir, file)

        if activate:
            if os.path.exists(offload_path):
                if os.path.exists(active_path):
                    remove(offload_path, offload_dir)
                else:
                    log('Activating: {}', os.path.join(dir, file))

                    rename(offload_path, active_path)
                    remove_empty_parents(offload_path, os.path.join(active_dir, dir))
        else:
            if os.path.exists(active_path):
                if os.path.exists(offload_path):
                    remove(offload_path, offload_dir)

                path = os.path.join(dir, file)
                log('Offloading: {}', path)

                rename(active_path, offload_path)


def script_main():
    try:
        main(**vars(parse_args()))
    except UserError as e:
        log('Error: {}', e.message)
    except KeyboardInterrupt:
        log('Operation interrupted.')
