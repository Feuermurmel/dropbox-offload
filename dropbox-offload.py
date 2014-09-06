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
		remove_empty_parent(path, remove_parents_up_to)


def rename(source, target):
	parent = os.path.dirname(target)
	
	if not os.path.exists(parent):
		os.makedirs(parent)
	
	os.rename(source, target)


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
	parser.add_argument('queue_dir')
	parser.add_argument('offload_dir')
	
	return parser.parse_args()


class Statistics:
	def __init__(self, queue_file_count, offload_file_count):
		self.queue_file_count = queue_file_count
		self.offload_file_count = offload_file_count
	
	def __eq__(self, other):
		return type(self) == type(other) and self._key == other._key
	
	@property
	def _key(self):
		return self.queue_file_count, self.offload_file_count
	
	def log(self):
		log('{} files total, {} offloaded.', self.queue_file_count + self.offload_file_count, self.offload_file_count)


def process_directories(offload_dir, queue_dir, count):
	top_level_dir_names = set(iter_child_dirs(queue_dir)) | set(iter_child_dirs(offload_dir))
	queue_file_count = 0
	offload_file_count = 0
	
	for top_level_dir in sorted(top_level_dir_names, key = numeric_sort_key):
		queue_top_level_dir = os.path.join(queue_dir, top_level_dir)
		offload_top_level_dir = os.path.join(offload_dir, top_level_dir)

		files = sorted(set(iter_files(queue_top_level_dir)) | set(
			iter_files(offload_top_level_dir)), key = numeric_sort_key)

		queue_files = files[:count]
		offload_files = files[count:]
		
		queue_file_count += len(queue_files)
		offload_file_count += len(offload_files)
		
		for i in queue_files:
			queue_path = os.path.join(queue_top_level_dir, i)
			offload_path = os.path.join(offload_top_level_dir, i)

			if os.path.exists(offload_path):
				if os.path.exists(queue_path):
					remove(offload_path, offload_dir)
				else:
					log('Activating: {}', os.path.join(top_level_dir, i))
					
					rename(offload_path, queue_path)
					remove_empty_parent(offload_path, offload_top_level_dir)

		for i in offload_files:
			queue_path = os.path.join(queue_top_level_dir, i)
			offload_path = os.path.join(offload_top_level_dir, i)

			if os.path.exists(queue_path):
				if os.path.exists(offload_path):
					remove(offload_path, offload_dir)
				
				log('Offloading: {}', os.path.join(top_level_dir, i))
				
				rename(queue_path, offload_path)
	
	return Statistics(queue_file_count, offload_file_count)


def main():
	args = parse_args()

	queue_dir = args.queue_dir
	offload_dir = args.offload_dir
	count = args.count
	
	if args.continuous:
		stat = None
		
		with dir_watcher_event(queue_dir) as watcher:
			while True:
				new_stat = process_directories(offload_dir, queue_dir, count)
				
				if new_stat != stat:
					stat = new_stat
					stat.log()
				
				watcher()
	else:
		stat = process_directories(offload_dir, queue_dir, count)
		stat.log()


try:
	main()
except UserError as e:
	log('Error: {}', e.message)
except KeyboardInterrupt:
	log('Operation interrupted.')
