#! /usr/bin/env python3

import sys, os, shutil, re, subprocess, argparse, contextlib, threading


class UserError(Exception):
	def __init__(self, msg, *args):
		self.message = msg.format(*args)


def log(msg, *args):
	print(msg.format(*args), file = sys.stderr)


def command(args):
	process = subprocess.Popen(args, stdout = subprocess.PIPE)
	process.communicate()
	
	assert not process.returncode


def read_file(path):
	with open(path, 'rb') as file:
		return file.read()


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


def remove_empty_parent(path, root):
	parent = os.path.dirname(path)
	
	if is_subdir_of(os.path.dirname(parent), root) and all(i.startswith('.') and not os.path.isdir(os.path.join(path, i)) for i in os.listdir(parent)):
		remove(parent, root)


def remove(path, remove_parents_up_to):
	if os.path.isdir(path):
		log('Removing directory: {}', path)
		
		shutil.rmtree(path)
	else:
		log('Removing: {}', path)
		
		os.unlink(path)
	
	if remove_parents_up_to is not None:
		remove_empty_parent(path, remove_parents_up_to)


def rename(source, target, remove_parents_up_to = None):
	parent = os.path.dirname(target)
	
	if not os.path.exists(parent):
		os.makedirs(parent)
	
	os.rename(source, target)
	
	if remove_parents_up_to is not None:
		remove_empty_parent(source, remove_parents_up_to)


def start_daemon_thread(target):
	thread = threading.Thread(target = target)
	
	thread.setDaemon(True)
	thread.start()


@contextlib.contextmanager
def dir_watcher_event(dir):
	if sys.platform == 'darwin':
		args = ['fsevents', '--bare', dir]
	elif sys.platform.startswith('linux'):
		args = ['inotifywait', '-rm', '-e', 'close_write,move,create,delete', '--format', '.', dir]
	else:
		raise UserError('Unknown platform: {}', sys.platform)
	
	process = subprocess.Popen(args, stdout = subprocess.PIPE)
	event = threading.Event()
	
	def target():
		while True:
			process.stdout.readline()
			event.set()

	start_daemon_thread(target)
	
	try:
		def fn():
			event.wait()
			event.clear()
		
		yield fn
	finally:
		process.kill()
		process.wait()


def parse_args():
	parser = argparse.ArgumentParser()
	
	parser.add_argument('-c', '--continuous', action = 'store_true', default = False)
	parser.add_argument('-n', '--count', type = int, default = 3)
	parser.add_argument('source_dir')
	parser.add_argument('offload_dir')
	
	return parser.parse_args()


def process_directories(offload_dir, source_dir, count):
	top_level_dir_names = set(iter_child_dirs(source_dir)) | set(iter_child_dirs(offload_dir))
	
	for top_level_dir in sorted(top_level_dir_names, key = numeric_sort_key):
		source_top_level_dir = os.path.join(source_dir, top_level_dir)
		offload_top_level_dir = os.path.join(offload_dir, top_level_dir)

		files = sorted(set(iter_files(source_top_level_dir)) | set(
			iter_files(offload_top_level_dir)), key = numeric_sort_key)

		source_files = files[:count]
		offload_files = files[count:]

		for i in source_files:
			source_path = os.path.join(source_top_level_dir, i)
			offload_path = os.path.join(offload_top_level_dir, i)

			if os.path.exists(offload_path):
				if os.path.exists(source_path):
					remove(offload_path, offload_dir)
				else:
					log('Activating: {}', os.path.join(top_level_dir, i))
					
					rename(offload_path, source_path, offload_dir)

		for i in offload_files:
			source_path = os.path.join(source_top_level_dir, i)
			offload_path = os.path.join(offload_top_level_dir, i)

			if os.path.exists(source_path):
				if os.path.exists(offload_path):
					remove(offload_path, offload_dir)
				else:
					log('Offloading: {}', os.path.join(top_level_dir, i))
					
					rename(source_path, offload_path)


def main():
	args = parse_args()

	source_dir = args.source_dir
	offload_dir = args.offload_dir
	count = args.count
	
	if args.continuous:
		with dir_watcher_event(source_dir) as watcher:
			while True:
				process_directories(offload_dir, source_dir, count)
				
				watcher()
	else:
		process_directories(offload_dir, source_dir, count)


try:
	main()
except UserError as e:
	log('Error: {}', e.message)
except KeyboardInterrupt:
	log('Operation interrupted.')