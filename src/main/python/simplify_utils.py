from geomet import wkt
from pprint import pprint
from shapely.wkt import loads
import json
import sys
import psycopg2
import os


class SimplifyUtils():


    def length(self, feature):
        count = 0
        for sub in feature:
            # print "sub = {0}".format(sub)
            if isinstance(sub, list):
                count = count + self.length(sub)
            else:
                count = count + 1
        return count

    def remove_dup_coords(self, geoJsonGeometry, error):
        print "remove_dup_coords, error=" + error
        m = re.search('consecutive coordinates at: \((.*), NaN\)', error)
        group = m.group(1).replace(" ", "")
        coords = "[" + group + "],[" + group + "]"
        print "removing duplicate coordinates: {0}".format(coords)

        str = json.dumps(geoJsonGeometry)
        new_wkt = str.replace(" ", "").replace(coords, "[" + group + "]")
        # print "new coordinates: {0}".format(new_wkt)
        return json.loads(new_wkt)


    def simplify(self, geometry):
        print geometry
        l = self.length(geometry['coordinates'])/2
        print "simplify - length: {0}".format(l)
        tolerance = 0.05
        loop = 0
        wkt_dumps = wkt.dumps(geometry)
        s = loads(wkt_dumps)
        simple = s
        max_length = 2000
        if l < max_length:
            print "not simplifying, < {} vertices".format(max_length)
            return simple
        while l > max_length and loop < 2:
            loop = loop + 1
            tolerance_loop = tolerance * loop
            simple = s.simplify(tolerance_loop)
        pprint(wkt.loads(simple))
        l = self.length(wkt.loads(simple)['coordinates'])/2
        print "simplified with t={0} , right now l={1}".format(tolerance_loop, l)
        s = loads(wkt.dumps(geometry))
        return simple


    def simplify_shape_using_postgis(self, geoJson):
        try:
          conn = psycopg2.connect(
            dbname=os.environ['PSQL_MAIN_DATABASE'],
            port=os.environ['PSQL_MAIN_PORT'],
            host=os.environ['PSQL_MAIN_HOST'],
            user=os.environ['PSQL_MAIN_USER'],
            password=os.environ['PSQL_MAIN_PASSWORD'])
          cur = self.conn.cursor()
          sql = "SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_GeomFromGeoJSON('{0}'), 0.01)) as a;".format(
            json.dumps(geoJson))
          print sql
          cur.execute(sql)
          corrected = cur.fetchall()[0][0]
          print "corrected" + corrected
          return json.loads(corrected)
        except psycopg2.Error as e:
          print("I am unable to connect to the database: {}".format(e))
          sys.exit(1)

    def remove_holes(geometry):
        if geometry['type'] == 'Polygon':
            coords = geometry['coordinates']
            # pprint(json.dumps(doc['shape']))
            newCoords = []
            newCoords.append(coords[0])
            print "removed {} holes".format(len(coords) - 1)
            geometry['coordinates'] = newCoords
        if geometry['type'] == 'MultiPolygon':
            coords = geometry['coordinates']
            # pprint(json.dumps(doc['shape']))
            newPols = []
            for i, pol in enumerate(coords):
                newPols.append([pol[0]])
                print "removed {} holes".format(len(pol) - 1)
            geometry['coordinates'] = newPols
        pprint(json.dumps(geometry))

