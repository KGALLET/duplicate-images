#!/usr/bin/env python3
"""
A tool to find and remove duplicate pictures.

Usage:
    duplicate_finder.py add <path> ...
    duplicate_finder.py remove <path> ...
    duplicate_finder.py clear
    duplicate_finder.py show
    duplicate_finder.py find [--delete] [--filename]
    duplicate_finder.py -h | --help

Options:
    -h, --help                Show this screen

    find:
        --delete              Move all found duplicate pictures to the trash. This option takes priority over --print.
        --filename            Get all filename of duplicates
"""

import concurrent.futures
from contextlib import contextmanager
import os
import magic
import math
from pprint import pprint
import shutil

import imagehash
from more_itertools import chunked
from PIL import Image, ExifTags
from tinydb import TinyDB, Query

@contextmanager
def connect_to_db(db_conn_string='./db'):
    if not os.path.isdir(db_conn_string):
        os.makedirs(db_conn_string)
    path_db = db_conn_string + "/db.json"
    db = TinyDB(path_db)

    yield db

def get_image_files(path):
    """
    Check path recursively for files. If any compatible file is found, it is
    yielded with its full path.

    :param path:
    :return: yield absolute path
    """
    def is_image(file_name):
        # List mime types fully supported by Pillow
        full_supported_formats = ['gif', 'jp2', 'jpeg', 'jpg', 'pcx', 'png', 'tiff', 'x-ms-bmp',
                                  'x-portable-pixmap', 'x-xbitmap']
        try:
            mime = magic.from_file(file_name, mime=True)
            print(mime)
            print(mime.rsplit('/', 1)[1] )
            return mime.rsplit('/', 1)[1] in full_supported_formats
        except IndexError:
            return False

    path = os.path.abspath(path)
    for root, dirs, files in os.walk(path):
        for file in files:
            file = os.path.join(root, file)
            if is_image(file):
                yield file

def hash_file(file):
    try:
        hashes = []
        img = Image.open(file)

        file_size = get_file_size(file)
        image_size = get_image_size(img)
        capture_time = get_capture_time(img)

        # hash the image 4 times and rotate it by 90 degrees each time
        for angle in [ 0, 90, 180, 270 ]:
            if angle > 0:
                turned_img = img.rotate(angle, expand=True)
            else:
                turned_img = img
            hashes.append(str(imagehash.phash(turned_img)))

        hashes = ''.join(sorted(hashes))

        print("\tHashed {}".format(file))
        return file, hashes, file_size, image_size, capture_time
    except OSError:
        print("\tUnable to open {}".format(file))
        return None

def hash_files_parallel(files):
        for result in map(hash_file, files):
            if result is not None:
                yield result

def _add_to_database(file_, hash_, file_size, image_size, capture_time, db):
    db.insert({"_id": file_,
               "hash": hash_,
               "file_size": file_size,
               "image_size": image_size,
               "capture_time": capture_time})

def _in_database(file, db):
    return db.count(Query()._id == file) > 0

def new_image_files(files, db):
    for file in files:
        if _in_database(file, db):
            print("\tAlready hashed {}".format(file))
        else:
            yield file

def add(paths, db):
    for path in paths:
        print("Hashing {}".format(path))
        files = get_image_files(path)
        files = new_image_files(files, db)

        for result in hash_files_parallel(files):
            _add_to_database(*result, db=db)

        print("...done")

def remove(paths, db):
    for path in paths:
        files = get_image_files(path)

        # TODO: Can I do a bulk delete?
        for file in files:
            remove_image(file, db)

def remove_image(file, db):
    db.remove(Query()._id == file)

def clear(db):
    db.purge()

def show(db):
    total = len(db.all())
    pprint(list(db.all()))
    print("Total: {}".format(total))

def same_time(dup):
    items = dup['items']
    if "Time unknown" in items:
        # Since we can't know for sure, better safe than sorry
        return True

    if len(set([i['capture_time'] for i in items])) > 1:
        return False

    return True

def find(db):
    dups = db.search(Query().hash != None)
    dupsHashsNotFiltered = [ dup['hash'] for dup in dups ]
    dupsHashsFiltered = set([dup for dup in dupsHashsNotFiltered if dupsHashsNotFiltered.count(dup) > 1])

    results = []
    for dup in dups:
        if dup['hash'] in dupsHashsFiltered:
            results.append(dup)
    return results

def delete_duplicates(duplicates, db):
    results = [delete_picture(x['file_name'], db)
               for dup in duplicates for x in dup['items'][1:]]
    print("Deleted {}/{} files".format(results.count(True),
                                        len(results)))

def delete_picture(file_name, db, trash="./Trash/"):
    print("Moving {} to {}".format(file_name, trash))
    if not os.path.exists(trash):
        os.makedirs(trash)
    try:
        shutil.move(file_name, trash + os.path.basename(file_name))
        remove_image(file_name, db)
    except FileNotFoundError:
        print("File not found {}".format(file_name))
        return False
    except Exception as e:
        print("Error: {}".format(str(e)))
        return False

    return True

def get_file_size(file_name):
    try:
        return os.path.getsize(file_name)
    except FileNotFoundError:
        return 0

def get_image_size(img):
    return "{} x {}".format(*img.size)

def get_capture_time(img):
    try:
        exif = {
            ExifTags.TAGS[k]: v
            for k, v in img._getexif().items()
            if k in ExifTags.TAGS
        }
        return exif["DateTimeOriginal"]
    except:
        return "Time unknown"

if __name__ == '__main__':
    from docopt import docopt
    args = docopt(__doc__)

    DB_PATH = "./db"
    TRASH = "./Trash/"

    with connect_to_db(db_conn_string=DB_PATH) as db:
        if args['add']:
            add(args['<path>'], db)
        elif args['remove']:
            remove(args['<path>'], db)
        elif args['clear']:
            clear(db)
        elif args['show']:
            show(db)
        elif args['find']:
            dups = find(db)

            if args['--delete']:
                delete_duplicates(dups, db)
            elif args['--filename']:
                pprint([ dup['_id'] for dup in dups ])
            else:
                pprint(dups)
                print("Number of duplicates: {}".format(len(dups)))


