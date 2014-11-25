#! /usr/bin/env python3

import sys, os, shutil, re, argparse, itertools, collections


class UserError(Exception):
	def __init__(self, msg, *args):
		self.message = msg.format(*args)


infinity = float('inf')


def log(msg, *args):
	print(msg.format(*args), file = sys.stderr)


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
	match = re.match('(?P<number>[0-9]+)(?P<suffix>[^0-9]?)$', arg)
	
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
	
	return int(match.group('number')) * factor


def parse_args():
	parser = argparse.ArgumentParser()
	
	parser.add_argument('-n', '--per-directory-limit', type = int, default = infinity)
	parser.add_argument('-N', '--global-limit', type = int, default = infinity)
	parser.add_argument('-m', '--global-minimum', type = int, default = 1)
	parser.add_argument('-s', '--size-limit', type = size_arg, default = infinity)
	parser.add_argument('queue_dir')
	parser.add_argument('offload_dir')
	
	args = parser.parse_args()
	
	if args.per_directory_limit == infinity and args.global_limit == infinity and args.size_limit == infinity:
		args.per_directory_limit = 3
	
	return args


def process_directories(offload_dir, queue_dir, per_directory_limit, global_limit, size_limit, global_minimum):
	files_by_dir = collections.defaultdict(list)
	size_by_dir_file = { }
	
	for i in [queue_dir, offload_dir]:
		for j in iter_child_dirs(i):
			for k in iter_files(os.path.join(i, j)):
				files_by_dir[j].append(k)
				size_by_dir_file[j, k] = get_size(os.path.join(i, j, k))
	
	def key(x):
		dir, file, successor_count = x
		
		# Prefer files with less succeeding files in the same directory, then files in directories with less files. Then sort by name and use the dir as a last resort ordering criterion.
		return -successor_count, len(files_by_dir[dir]), numeric_sort_key(file), dir
	
	ordered_files = sorted(((dir, i, c) for dir, files in files_by_dir.items() for c, i in enumerate(reversed(sorted(files, key = numeric_sort_key)))), key = key)
	
	files = []
	count_by_directory = collections.defaultdict(lambda: per_directory_limit)
	
	for dir, file, _ in ordered_files:
		size = size_by_dir_file[dir, file]
		
		if global_minimum > 0 or global_limit > 0 and count_by_directory[dir] > 0 and size_limit >= size:
			global_minimum -= 1
			global_limit -= 1
			count_by_directory[dir] -= 1
			size_limit -= size
			
			offload = False
		else:
			offload = True
		
		files.append((offload, dir, file))
	
	for offload, dir, file in files:
		queue_path = os.path.join(queue_dir, dir, file)
		offload_path = os.path.join(offload_dir, dir, file)
		
		if offload:
			if os.path.exists(queue_path):
				if os.path.exists(offload_path):
					remove(offload_path, offload_dir)
				
				path = os.path.join(dir, file)
				log('Offloading: {}', path)
				
				rename(queue_path, offload_path)
		else:
			if os.path.exists(offload_path):
				if os.path.exists(queue_path):
					remove(offload_path, offload_dir)
				else:
					log('Activating: {}', os.path.join(dir, file))
					
					rename(offload_path, queue_path)
					remove_empty_parents(offload_path, os.path.join(queue_dir, dir))


def main():
	process_directories(**vars(parse_args()))


try:
	main()
except UserError as e:
	log('Error: {}', e.message)
except KeyboardInterrupt:
	log('Operation interrupted.')
