#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pyback.py: Efficient backup of data.

This tool backs up data using a hardlink mechanism similar to the rsync tool.

TODO:
- Refactor source
- Improve robustness by handling exceptions from file operations
- Allow multiple source folder and record the sources in the backup
- Cache hashes of files, so incremental backups don't need to read all files from the backup medium for comparison (slow!)
- Find a way to continue interrupted backups (maybe also use tmp folder as bases?)
- Improve output and progress display
- Catch events and provide some kind of ordered shutdown mechanism
"""


__author__      = "Andreas Gäer"
__copyright__   = "Copyright 2015, Andreas Gäer"

import argparse
import os
import shutil
import filecmp
import datetime
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed

def get_file_count(path):
	'''
	Get count of files in path
	'''

	count = 0
	for root, dirnames, filenames in os.walk(path):
		count = count + len(filenames)
	return count

try:
	# Try to use tqdm (https://github.com/noamraph/tqdm) as progress display
	from tqdma import tqdm as progress_printer
except ImportError:
	# tqdm not available, use a very simple version
	def progress_printer(it, total = None):
		'''Helper to display the progress of longer running operations'''

		width = 40
		done = 0
		last_progress = 0
		for obj in it:
			yield obj
			done = done + 1
			progress = round((width * done) / total)
			if progress > last_progress:
				left = width - progress
				sys.stdout.write("\r |" + "#" * progress + " " * left + "| %d / %d" % (done, total))
				sys.stdout.flush()
				last_progress = progress
		sys.stdout.write("\n")
		sys.stdout.flush()

class BackupFolder:
	'''Class to handle the target folder for backups'''

	# How to format the timestamp when creating sub-folder
	timeformat = '%Y%m%d_%H%M%S%f'
	
	# How to mark temporary (uncomplete) backup folder
	tmp_suffix = ".tmp"
	
	def __init__(self, backup_root):
		'''
		Initialize
		@param backup_root: Folder under that all backups will live, must exist
		'''
		self.root = os.path.realpath(backup_root)
		if not os.path.exists(self.root):
			raise RuntimeError("Backup root '%s' does not exist!" % self.root)
			

	def make_timestamped_subdirname(self):
		'''
		Make a foldername under self.root based on the current timestamp.
		This will not actually create any subdirectories, but only create a name.
		@return a tuple of the subdirectory path name and the path name for an temporary folder
		'''
		ts = datetime.datetime.utcnow()
		sub = os.path.join(self.root, ts.strftime(self.timeformat))
		tmp = sub + self.tmp_suffix
		return (sub, tmp)

	def find_newest_subdirname(self):
		'''
		Find the newest subdirectory name. Do not consider temporary folder.
		@return name of the newest subdirectory
		'''
		newest = (None, None)
		for e in os.listdir(self.root):
			sub = os.path.join(self.root, e)
			if not os.path.isdir(self.root): continue
			if e.endswith(self.tmp_suffix): continue
			try:
				dt = datetime.datetime.strptime(e, self.timeformat)
				if newest[0] is None or dt > newest[0]:
					newest = (dt, sub)
			except:
				raise
		return os.path.normpath(newest[1]) if newest[1] is not None else None

	def remove_paths(self, path_list):
		'''
		Remove a list of folders recursivly. But check that those folder
		life under our own root.
		@param path_list: list of paths to be removed
		'''
		
		def path_below(sub, root):
			'''
			Check that sub is below root
			Algorithm: We split of path parts from the end until the rest of sub is equal root
			'''
			
			# Normalize both paths first
			s = os.path.realpath(sub)
			r = os.path.realpath(root).rstrip(os.sep)
			
			# Now strip and check
			while s != "":
				(s, _) = os.path.split(s)
				if r == s: return True
			return False
				
		for p in path_list:
			if path_below(p, self.root):
				print("remove:", p)
				pp = progress_printer(p)
				for root, dirs, files in os.walk(p, topdown=False):
					for name in files:
						os.unlink(os.path.join(root, name))
						pp.pinc()
					for name in dirs:
						os.rmdir(os.path.join(root, name))
				os.rmdir(p)
				pp.close()

	def list_tmp(self):
		'''Generate a list of all subdirs of self.root that end with the tmp suffix'''
		for e in os.listdir(self.root):
			sub = os.path.join(self.root, e)
			if sub.endswith(self.tmp_suffix):
				yield sub

	def rm_tmp(self):
		'''Remove all temporary subdirectories'''
		self.remove_paths(self.list_tmp())
		
	def list_older(self, age = datetime.timedelta(weeks = 4)):
		'''Generate a list of all subdirectories with a timestamp name older age'''
		ref = datetime.datetime.utcnow() - age
		for e in os.listdir(self.root):
			sub = os.path.join(self.root, e)
			if not os.path.isdir(self.root): continue
			if e.endswith(self.tmp_suffix): continue
			dt = datetime.datetime.strptime(e, self.timeformat)
			if dt < ref:
				yield sub
				
	def rm_older(self, age = datetime.timedelta(weeks = 4)):
		'''Remove all subdirectories with a timestamp name older age'''
		self.remove_paths(self.list_older(age))

from hashlib import sha256
def hashcmp(f1, f2):
	'''
	Compare two files by using hash digests of the files.
	'''
	
	def hash_file(f):
		chunk_size = 64*1024
		h = sha256()
		with open(f, "rb") as f:
			for chunk in iter(lambda: f.read(chunk_size), b''):
				h.update(chunk)
		return h.digest()

	d1 = hash_file(f1)
	d2 = hash_file(f2)
	return d1 == d2
	
class CopyOrLink:

	def __init__(self, src, dst, base = None, verbose = False):
		self.src = src
		self.dst = dst
		self.base = base
		self.verbose = verbose

	def copy_or_link_file(self, sfile, dfile, bfile):
		
		# First test if the file from base could be re-used and linked
		# TODO: We could speed this up if we knew the sha256 of the base files and just check for equal sha256
		if bfile is not None and os.path.exists(bfile):
			if filecmp.cmp(bfile, sfile, shallow=False):
				os.link(bfile, dfile)
				return "link: %s => %s" % (bfile, dfile)
					
		# Nope, we need to copy the file
		shutil.copy2(sfile, dfile)
		return "copy: %s => %s" % (sfile, dfile)

	def create_subdir(self, d):
		os.makedirs(d)
		return "create: %s" % d

	def copy_or_link(self):
		if os.path.exists(self.dst):
			raise RuntimeError("Destination already exists!")
		print("Create destination: %s" % self.dst)
		os.makedirs(self.dst)
		total = get_file_count(self.src)
		with ThreadPoolExecutor(max_workers = 8) as tpe:
			work = []
			for root, dirnames, filenames in os.walk(self.src):
				relroot = os.path.relpath(root, self.src)
				for d in dirnames:
					ddir = os.path.normpath(os.path.join(self.dst, relroot,d))
					work.append(tpe.submit(self.create_subdir, ddir))
				for f in filenames:
					sfile = os.path.normpath(os.path.join(root, f))
					dfile = os.path.normpath(os.path.join(self.dst, relroot, f))
					bfile = os.path.normpath(os.path.join(self.base, relroot, f)) if base is not None else None
					work.append(tpe.submit(self.copy_or_link_file, sfile, dfile, bfile))
			for f in progress_printer(as_completed(work), total=total):
				if self.verbose:
					print(f.result())

def existing_directory(path):
	if not os.path.exists(path):
		raise argparse.ArgumentTypeError("'%s' does not exist" % path)
	return path

if __name__ == "__main__":

	# Command line parser
	parser = argparse.ArgumentParser(description='backup files')
	parser.add_argument('src', metavar='source', type=existing_directory,
					   help='source directory')
	parser.add_argument('dst', metavar='destination', type=existing_directory,
					   help='destination directory')
	parser.add_argument('-o', metavar='days', help='remove old backups', type=int)
	parser.add_argument('-v', action='store_true', help='verbose output')
	parser.add_argument('--sure', action='store_true', help='This tool is experimental. Add this option if you are sure you want to use it...')
	
	args = parser.parse_args()
	
	# This is still alpha, make sure people don't just use it...
	if not args.sure:
		print("Nothing done, check help...")
		sys.exit(0)
	else:
		print()
		print("=" * 5 + "WARNING" + "=" * 5)
		print("This tool is experimental!")
		print("Better don't use it to make backups of data you love!")
		print("=" * 5 + "WARNING" + "=" * 5)
		print()

	# What to backup
	# TODO: Multiple sources needed!
	src = os.path.normpath(args.src)

	# Get us a subdirectory name for this backup run
	bf = BackupFolder(args.dst)
	dst, tmp_dst = bf.make_timestamped_subdirname()
	base = bf.find_newest_subdirname()

	print("Backup from:", src)
	print("Backup to:  ", dst)
	print("Based on:   ", base)

	# Now run the backup
	col = CopyOrLink(src, tmp_dst, base, args.v)
	col.copy_or_link()
	os.rename(tmp_dst, dst)

	# Now we have a new backup, we can do a little garbage collection
	print("Remove leftover temporary folder")
	bf.rm_tmp()
		
	# Remove backups that are older than args.older days if requested
	print("Remove old backup folder")
	if args.o:
		bf.rm_older(datetime.timedelta(days=args.o))
		
	print("Done.")
