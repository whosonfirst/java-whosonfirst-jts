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

custom_geo_shapes = {
    # new zealand
    '85633345': {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [
                    [167.9675, -46.6998],
                    [168.1792, -46.8644],
                    [168.1921, -47.0989],
                    [167.9019, -47.1884],
                    [167.6458, -47.2052],
                    [167.6482, -47.048],
                    [167.8166, -46.8974],
                    [167.7874, -46.6972],
                    [167.9675, -46.6998]
                ]
            ],
            [
                [
                    [172.9045, -40.5085],
                    [172.7578, -40.5177],
                    [172.6891, -40.7341],
                    [172.8565, -40.8437],
                    [173.065, -40.8867],
                    [173.0309, -41.1491],
                    [173.2127, -41.3019],
                    [173.5711, -41.0681],
                    [174.2114, -40.9996],
                    [174.2604, -41.1183],
                    [174.0555, -41.4246],
                    [174.2259, -41.7234],
                    [174.1979, -41.8761],
                    [174.0041, -42.0312],
                    [173.8909, -42.2408],
                    [173.5586, -42.4911],
                    [173.2873, -42.9539],
                    [172.8112, -43.169],
                    [172.7297, -43.4148],
                    [172.804, -43.611],
                    [173.0529, -43.6533],
                    [173.1055, -43.8294],
                    [172.8767, -43.902],
                    [172.4971, -43.7219],
                    [172.3656, -43.8684],
                    [172.0612, -43.9566],
                    [171.5506, -44.1673],
                    [171.346, -44.2786],
                    [171.196, -44.5668],
                    [171.2031, -44.9055],
                    [170.9803, -45.155],
                    [170.9063, -45.4028],
                    [170.6984, -45.6793],
                    [170.7873, -45.8631],
                    [170.3338, -45.9815],
                    [170.2349, -46.154],
                    [169.8648, -46.3784],
                    [169.849, -46.4675],
                    [169.6351, -46.5784],
                    [169.036, -46.6819],
                    [168.8449, -46.5602],
                    [168.5915, -46.6144],
                    [168.3657, -46.5402],
                    [168.2161, -46.3544],
                    [167.8501, -46.3966],
                    [167.6979, -46.1887],
                    [167.4609, -46.1482],
                    [167.4058, -46.2512],
                    [166.762, -46.22],
                    [166.4694, -45.9835],
                    [166.4898, -45.8075],
                    [166.7975, -45.7244],
                    [166.7025, -45.5424],
                    [166.8814, -45.2796],
                    [167.2035, -44.95],
                    [167.4187, -44.8409],
                    [168.3401, -44.1183],
                    [168.4103, -44.0279],
                    [168.6458, -43.9645],
                    [168.7468, -44.0103],
                    [169.1921, -43.7587],
                    [169.7337, -43.5657],
                    [169.8968, -43.3945],
                    [170.2388, -43.2379],
                    [170.5144, -43.0066],
                    [170.7007, -42.9547],
                    [171.1228, -42.5924],
                    [171.3049, -42.2946],
                    [171.3316, -42.1472],
                    [171.4888, -41.8303],
                    [171.9038, -41.6069],
                    [172.0708, -41.4142],
                    [172.1144, -41.2509],
                    [172.1068, -40.8869],
                    [172.6414, -40.5097],
                    [172.9045, -40.5085]
                ]
            ],
            [
                [
                    [173.0793, -34.4117],
                    [172.9075, -34.5414],
                    [173.0508, -34.6013],
                    [173.1231, -34.8097],
                    [173.4783, -34.9904],
                    [173.8882, -35.006],
                    [174.1375, -35.3191],
                    [174.3325, -35.288],
                    [174.5695, -35.5943],
                    [174.5174, -35.7208],
                    [174.5793, -35.8495],
                    [174.4969, -35.9876],
                    [174.757, -36.2449],
                    [174.7229, -36.5937],
                    [174.8191, -36.8237],
                    [175.241, -36.9423],
                    [175.3311, -37.0213],
                    [175.3672, -37.2072],
                    [175.5957, -37.2131],
                    [175.5278, -36.9625],
                    [175.5025, -36.6539],
                    [175.7857, -36.7828],
                    [175.8818, -36.926],
                    [175.9972, -37.6318],
                    [176.5186, -37.7621],
                    [176.7183, -37.8777],
                    [177.1455, -38.0235],
                    [177.5568, -37.9138],
                    [177.7388, -37.6762],
                    [178.0315, -37.5496],
                    [178.2777, -37.5593],
                    [178.5643, -37.7177],
                    [178.3516, -38.0017],
                    [178.2946, -38.5398],
                    [177.964, -38.7614],
                    [177.8828, -39.0855],
                    [177.9989, -39.1298],
                    [177.8685, -39.2902],
                    [177.7639, -39.081],
                    [177.3874, -39.0774],
                    [177.065, -39.1994],
                    [176.9063, -39.4475],
                    [177.0364, -39.7536],
                    [176.8373, -40.1779],
                    [176.7036, -40.2902],
                    [176.348, -40.6965],
                    [176.2204, -40.9333],
                    [175.9668, -41.247],
                    [175.3303, -41.6098],
                    [175.1818, -41.4179],
                    [174.8656, -41.4195],
                    [174.6094, -41.2919],
                    [174.9427, -40.9706],
                    [175.1413, -40.6744],
                    [175.2349, -40.336],
                    [175.155, -40.0854],
                    [174.9673, -39.9164],
                    [174.5676, -39.8268],
                    [174.319, -39.6139],
                    [173.9866, -39.5423],
                    [173.7844, -39.3932],
                    [173.8527, -39.1345],
                    [174.2116, -38.9733],
                    [174.3667, -38.9757],
                    [174.5789, -38.8395],
                    [174.7051, -38.1197],
                    [174.8504, -37.7649],
                    [174.6917, -37.3294],
                    [174.7043, -37.176],
                    [174.8805, -37.0763],
                    [174.8328, -36.9428],
                    [174.4889, -37.0146],
                    [174.3599, -36.6313],
                    [174.4697, -36.5834],
                    [174.4414, -36.4065],
                    [174.2923, -36.32],
                    [174.0798, -36.3995],
                    [173.8582, -36.0656],
                    [173.1003, -35.22],
                    [173.202, -35.0578],
                    [173.1436, -34.9284],
                    [172.7993, -34.551],
                    [172.7417, -34.4312],
                    [173.0793, -34.4117]
                ]
            ]
        ]
    }
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
        # try:
        #   res = es.get(wofs_index_name, id, wof_doc_type, _source=False)
        #   print "found."
        #   continue
        # except NotFoundError, error:
        #     pprint(error)
        print("not found {0}, creating.".format(id))
        geometry_ = data['geometry']
        if (id in custom_geo_shapes):
            print("custom shape for {0}".format(id))
            geometry_ = custom_geo_shapes[id]
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
            # "wof_hierarchy": properties.get('wof:hierarchy:', []),
            # "wof_belongsto": properties.get('wof:belongsto', []),
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
            done = False
            tries = 0;
            while not done and tries < 5:
                tries = tries + 1
                try:
                    print "indexing {0}, {1}".format(wofs_index_name, es)
                    # simplify_shape(doc)
                    # if (id in remove_holes_shapes):
                    #     remove_holes(doc)
                    # pprint(json.dumps(doc))
                    res = es.index(index=wofs_index_name, doc_type=wof_doc_type, id=id, body=doc,
                                   request_timeout=360)
                    pprint(res)
                    done = True
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
                        done = True
                        error_ = [id, name, reason_]
                        print error_
                        failed_shapes.append(error_)

                        # except elasticsearch.exceptions.ConnectionTimeout as timeout:
                        # try to simplify to make the indexing easier
                        # print(
                        # "simplifying original shape with tolerance {0} ...".format(shape_tolerance))
                        # doc['shape'] = wkt.loads((shape.simplify(shape_tolerance))),
                        # shape_tolerance = shape_tolerance + 0.01
                        # print "got timeout, trying again ..."
                        # time.sleep(10)
                        # except elasticsearch.exceptions.ConnectionError as e:
                        #     pprint(e)
            if tries == 5:
                error_ = [id, name, "tries =5"]
                print error_
                failed_shapes.append(error_)
                print "new error. checked {0} files, imported {1} records. Failed: \n {2}".format(
                    filecount, count, failed_shapes)

                # data_file.close()


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
