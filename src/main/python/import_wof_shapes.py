#!python

import json
import os
import sys
from urllib2 import urlopen
from contextlib import contextmanager
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, NotFoundError
from geomet import wkt
from pprint import pprint
from shapely.wkt import loads

from simplify_utils import SimplifyUtils

reload(sys)
sys.setdefaultencoding('utf8')

els_server = "http://{}:{}".format(os.environ['ELS_MAIN2_HOST'], os.environ['ELS_MAIN2_PORT'])
print "Connecting to ELS server {0}".format(els_server)
es = Elasticsearch([els_server], timeout=60)
simplifyUtils = SimplifyUtils()

wofs_index_name = 'wofs'
wof_doc_type = "wof"

# place_types = ['country', 'county', 'region', 'locality', 'neighbourhood']
place_types = ['region']

place_type_weights = {
    'country': 100,
    'region': 70,
    'county': 50,
    'locality': 30,
    'neighbourhood': 10
}

remove_holes_shapes = [
    # '85632505',  # argentina
    # '85632475',  # bangladesh
    # '85632469',  # india
    # '85632761',  # kyrgyzstan
    # '85632449',  # ivory Coast
    # '85632793',  # australia
    # '85632773',  # armenia
    # '101752293',  # boras
]

skiplist = [
]

wof_dir = '../whosonfirst-data/data/'
# wof_dir = './'
failed_shapes = []

import_dirs = ['823', '856', '857', '858', '859', '874', '890', '907', '101', '102', '136', '404', '420', '421']
# import_dirs = ['890', '907', '101', '102', '136', '404', '420', '421']
# import_dirs = ['856/331/43']  # finland
# import_dirs = ['856/324/75']  # finland
failed = []

filecount = 0


def addIfExists(name_inputs, properties, names, doc_id):
    names_ = properties.get(names, None)
    if names_ is not None:
        # print 'appeding {0}, {1}'.format(names_, type(names_))
        if type(names_) in [str, unicode]:
            if not any(names_ in s for s in name_inputs):
                # print 'appending str {0}'.format(names_)
                name_inputs.append(names_)  # + doc_id)
        else:
            for name in names_:
                # print 'item {0}'.format(name)
                if not any(name in s for s in name_inputs):
                    name_inputs.append(name)  # + doc_id)
    return name_inputs


if len(sys.argv) > 2:
    wof_dir = sys.argv[2]


@contextmanager
def stdout_redirected(new_stdout):
    save_stdout = sys.stdout
    sys.stdout = new_stdout
    try:
        yield None
    finally:
        sys.stdout = save_stdout


def import_geojson(data):
    if not data.get('properties'):
        print "no properties, skipping {0}".format(data)
        return
    properties = data['properties']
    placetype_ = properties['wof:placetype']
    if not placetype_ in place_types:
        print "skipping, placetype = {0}".format(placetype_)
        return
    print properties
    name = unicode(properties['wof:name'])
    id = data['id']
    pprint("Checking {0}, {1}, {2}".format(placetype_, id, name))
    if not data.get('geometry'):
        print "no geometry, skipping {0}".format(name)
        return
    else:
        print("creating {}".format(id))
        geometry_ = data['geometry']
        simple = simplifyUtils.simplify(geometry_)
        wkt_loads = wkt.loads(simple)
        name_inputs = []
        for nme in ["wof:name", "qs:a0", "qs:a0_alt", "qs:adm0", "ne:sovereignt",
                    "ne:name_long",
                    "ne:name_sort",
                    "ne:name", "ne:iso_a2", "ne:iso_a3", "ne:gu_a3", "ne:brk_name"]:
            name_inputs = addIfExists(name_inputs, properties, nme, id)
        for key in properties:
            # print key
            if key.startswith('name:'):
                addIfExists(name_inputs, properties, key, id)

        shape = loads(wkt.dumps(geometry_))
        bboxs = properties.get('geom:bbox').split(',')
        bbox = []
        for num in bboxs:
            bbox.append(float(num))
        pprint(bbox)
        doc = {
            'wof_name': name,
            'wof_placetype': placetype_,
            "wof_parent_id": properties.get('wof:parent_id', None),
            'suggest_name': {
                'input': name_inputs,
                # 'output': name,
                'weight': place_type_weights[placetype_],
                'context': {
                    "placetype": placetype_
                },
                'payload': {
                    "id": id,
                    "type": placetype_,
                    "shape_simple": wkt_loads,
                    "bbox": {
                        "type": "Polygon",
                        "coordinates": [
                            [bbox[0], bbox[1]],
                            [bbox[2], bbox[1]],
                            [bbox[2], bbox[3]],
                            [bbox[0], bbox[3]],
                            [bbox[0], bbox[1]]
                        ]
                    },
                    'iso_country': properties['iso:country']

                }
            },
            'iso_country': properties['iso:country']
        }
        doc['shape'] = wkt.loads(shape)
        if (placetype_ in place_types):
            try:
                print "indexing {0}, {1}".format(wofs_index_name, es)
                simplifyUtils.simplify_shape_using_postgis(doc['shape'])
                if (id in remove_holes_shapes):
                    simplifyUtils.remove_holes(doc['shape'])
                pprint(json.dumps(doc))
                res = es.index(index=wofs_index_name, doc_type=wof_doc_type, id=id, body=doc,
                               request_timeout=360)
                pprint(res)
            except RequestError as e:
                pprint(e)
                reason_ = e.info['error']['caused_by']['reason']
                pprint(reason_)
                sys.exit(1)
                if reason_ and "consecutive coordinates at: (" in reason_:
                    # try to remove dupe coords form the shapes
                    doc['suggest_name']['payload']['shape_simple'] = simplify_utils.remove_dup_coords(
                        doc['suggest_name']['payload']['shape_simple'], reason_.encode("ascii"))
                    doc['shape'] = remove_dup_coords(doc['shape'], reason_.encode("ascii"))
                elif reason_ and "LinearRing do not form a closed linestring" in reason_:
                    print "1"
                    simplify_utils.simplify_shape(doc)
                elif reason_ and "Invalid polygon, interior cannot share more than one point with the exterior" in e.error:
                    print "2"
                    simplify_utils.simplify_shape(doc)
                elif reason_ and "Self-intersection" in reason_:
                    print "self intersection"
                    simplify_utils.simplify_shape(doc)
                elif (reason_ and "-1" in reason_) or e.info['error']['caused_by'][
                    'type'] == 'array_index_out_of_bounds_exception':
                    print "probably holes error"
                    remove_holes(doc)
                else:
                    print "4"
                    error_ = [id, name, reason_]
                    print error_
                    failed_shapes.append(error_)


def import_wofs_from_url_file():
    for placetype in place_types:
        for line in open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      "../../test/resources/data/{}.txt".format(placetype))):
            print line
            text = urlopen(line).read().decode('utf8')
            data = json.loads(text)
            import_geojson(data)
            # filecount = filecount + 1


def import_wofs_from_repo():
    for dir in import_dirs:

        data_dir = wof_dir + dir
        print "importing from " + data_dir
        for root, sub_folders, files in os.walk(data_dir):
            for file in sorted(files):
                id = os.path.splitext(file)[0]
                print  root + " - " + id
                if "alt" in id or id in skiplist:
                    continue
                else:
                    print "{1}: checking {0}".format(id, filecount)
                    if not file.lower().endswith("json"):
                        print "cont"
                        continue
                    with open(os.path.join(root, file)) as data_file:
                        try:
                            import_geojson(json.load(data_file))
                        except ValueError, e:
                            pprint(e)
                            failed_shapes.append([id, "could not parse JSON", e])
                            print "parse error"
                            continue

        print "checked {0} files, imported {1} records. Failed: \n {2}".format(filecount, count, failed_shapes)


# import_wofs_from_repo()
import_wofs_from_url_file()
