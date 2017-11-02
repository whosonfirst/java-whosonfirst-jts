#!python

import json
import os
import sys
from pprint import pprint

reload(sys)
sys.setdefaultencoding('utf8')

base_url='https://raw.githubusercontent.com/whosonfirst-data/whosonfirst-data/master/data/'

# the relative path of your data-repo relative to this file
wof_dir = '/../../../../whosonfirst-data/data/'

script_path = os.path.dirname(os.path.realpath(__file__))

# the import dirs considered
import_dirs = ['823', '856', '857', '858', '859', '874', '890', '907', '101', '102', '136', '404', '420', '421']

# add more placetypes to generate more lists
placetypes=['region']


def generate_wof_lists():
    filecount = 0
    for dir in import_dirs:

        data_dir = script_path+ wof_dir + dir
        print "importing from " + data_dir
        for root, sub_folders, files in os.walk(data_dir):
            for file in sorted(files):
                id = os.path.splitext(file)[0]
                print  root + " - " + id
                filecount = filecount + 1
                if "alt" in id:
                    continue
                else:
                    print "{1}: checking {0}".format(id, filecount)
                    if not file.lower().endswith("json"):
                        print "cont"
                        continue
                    with open(os.path.join(root, file)) as data_file:
                        try:
                            data = json.load(data_file)
                        except ValueError, e:
                            pprint(e)
                            failed_shapes.append([id, "could not parse JSON", e])
                            print "parse error"
                            continue
                        if not data.get('properties'):
                            print "no properties, skipping {0}".format(file)
                            continue
                        properties = data['properties']
                        placetype_ = properties['wof:placetype']
                        if not placetype_ in placetypes:
                            print("skipping {}".format(placetype_))
                            continue
                        line = "{}/{}".format(root.replace(wof_dir, base_url), file)
                        print(line)
                        with open (script_path+"/../../test/resources/data/{}.txt".format(placetype_), 'a') as f:
                            f.write (line+"\n")

generate_wof_lists()