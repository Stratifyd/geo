"""
  @coding: utf-8
  @copyright: Unpublished Copyright (c) 2013-2016 [TASTE ANALYTICS, LLC]
  @license: All Rights Reserved
  @note: All information contained herein is, and remains the property of
    Taste Analytics LLC ("COMPANY"). The intellectual and technical concepts
    contained herein are proprietary to COMPANY and may be covered by U.S. and
    Foreign Patents, patents in process, and are protected by trade secret or
    copyright law. Dissemination of this information or reproduction of this
    material is strictly forbidden unless prior written permission is obtained
    from COMPANY. Access to the source code contained herein is hereby
    forbidden to anyone except current COMPANY employees, managers or
    contractors who have executed confidentiality and Non-disclosure agreements
    explicitly covering such access.

  @author: Christian Gibson
  @contact: cgibson@tasteanalytics.com
  @project: Taste Analytics Computation Node-- Front End GeoCode Helper

  @version: 1.04-sigma
  @updated: September 30, 2016
  @requires: python 2.7.10
"""

try:
    from cfuzzyset import cFuzzySet
except:
    cFuzzySet = set
from collections import Container
from helpers import GeoTree
from json import load, loads, dump, dumps
from math import ceil, log10
from operator import itemgetter
from os import remove
from os.path import exists, isfile
from pymongo import CursorType
from pymongo.collection import Collection
from requests import get as r_get
from sh import Command
from ta_common.field_names import RO, MC
from ta_common.geo.mapping import (countries as COUNTRY_MAPPING,
                                   regions as REGION_MAPPING,
                                   phone_codes as PHONE_MAPPING)
from ta_common.mango.relational_object import mutabledotdict
from time import time
from traceback import format_exc

try:
    from osgeo.ogr import CreateGeometryFromJson, Geometry, wkbPoint

    def Point(longitude, latitude, altitude=0.0):
        point = Geometry(wkbPoint)
        point.AddPoint(longitude, latitude, altitude)
        return point

    def create_from_geojson(dictionary):
        return CreateGeometryFromJson(dumps(dictionary))

    def distance(geo, other):
        return geo.Distance(other)

    def contains(geo, other):
        return not geo.Disjoint(other)

    def transform_to_geojson(geo):
        return geo.ExportToJson()

    def centroid(geo):
        return geo.Centroid().GetPoint()[:2][::-1]
except:
    gdal_error = format_exc()
    try:
        from shapely.geometry import shape, mapping, Point

        def create_from_geojson(dictionary):
            return shape(dictionary)

        def contains(geo, other):
            return geo.contains(other)

        def distance(geo, other):
            return geo.distance(other)

        def transform_to_geojson(geo):
            return mapping(geo)

        def centroid(geo):
            return shape(geo).centroid.coords[0][::-1]
    except:
        raise ImportError(
            u"GeoMapping object failed to instantiate;\nGDAL: %s\n\nShapely: %s"
            % (gdal_error, format_exc()))


class FuzzyMapping(object):

    def __init__(self):
        self.__set = cFuzzySet()
        self.__map = dict()
        self.__fxn = itemgetter(0)

    def __getitem__(self, key):
        _best, stored, _conf = self.get_stored_tuple(key)
        return stored

    def get_confidence(self, key):
        _best, _stored, conf = self.get_stored_tuple(key)
        return conf

    def get_best_match(self, key):
        best, _stored, _conf = self.get_stored_tuple(key)
        return best

    def get_stored_tuple(self, key):
        try:
            return (key, self.__map[key], 1.0)
        except:
            conf, item = max(self.__set.get(key), key=self.__fxn)
            return (item, self.__map[item], conf)

    def __setitem__(self, key, val):
        self.__set.add(key)
        self.__map[key] = val


class CentroidUpdateHelper(object):

    @classmethod
    def regenerate_mongo_cache(cls, nominatim_host, mongo_client):
        collection = mongo_client[MC.APP_DB]

        country_geocode = cls.regenerate_country_geocode(nominatim_host)
        cls.push_to_mongo(collection[MC.COUNTRY_COL], country_geocode)

        region_geocode = cls.regenerate_country_geocode(nominatim_host)
        cls.push_to_mongo(collection[MC.REGION_COL], region_geocode, depth=1)

    @classmethod
    def pull_from_mongo(cls, collection):
        dictionary = mutabledotdict()
        for doc in collection.find(cursor_type=CursorType.EXHAUST):
            dictionary[doc[RO.OBJECT_ID]] = doc['val']
        return dictionary.to_dict()

    @classmethod
    def push_to_mongo(cls, collection, dictionary, depth=None, _safe=False):
        depth = depth if isinstance(depth, int) and depth > 0 else 0
        dictionary = mutabledotdict(dictionary)
        if _safe:
            seen = set()
            for key in dictionary.rawkeys(mindepth=depth, maxdepth=depth):
                seen.add(key)
                print collection.update(
                    {RO.OBJECT_ID: key},
                    {'$set': {'val': dictionary[key].to_dict()}},
                    upsert=True)
            for rev in reversed(range(depth)):
                seen = set('.'.join(k.split('.')[:-1]) for k in seen)
                for key in dictionary.rawkeys(mindepth=rev, maxdepth=rev):
                    if key in seen:
                        continue
                    seen.add(key)
                    print collection.update(
                        {RO.OBJECT_ID: key},
                        {'$set': {'val': dictionary[key].to_dict()}},
                        upsert=True)
        else:
            seen = set()
            bulk = collection.initialize_ordered_bulk_op()
            for key in dictionary.rawkeys(mindepth=depth, maxdepth=depth):
                seen.add(key)
                bulk.find({RO.OBJECT_ID: key}).upsert().update(
                    {'$set': {'val': dictionary[key].to_dict()}})
            for rev in reversed(range(depth)):
                seen = set('.'.join(k.split('.')[:-1]) for k in seen)
                for key in dictionary.rawkeys(mindepth=rev, maxdepth=rev):
                    if key in seen:
                        continue
                    seen.add(key)
                    bulk.find({RO.OBJECT_ID: key}).upsert().update(
                        {'$set': {'val': dictionary[key].to_dict()}})
            bulk.execute()

    @classmethod
    def regenerate_country_geocode(cls, nominatim_host, verbose=False):
        country_geocode = {}
        helper = CentroidUpdateHelper(nominatim_host)
        idx, tot = 1, len(COUNTRY_MAPPING)
        fmt = '%%0%dd' % int(ceil(log10(tot)))
        fmt = 'Finished %s / %s :: %%s' % (fmt, fmt)
        for code, name in COUNTRY_MAPPING.iteritems():
            country_geocode[code] = helper(name)
            if verbose:
                print fmt % (idx, tot, name)
            idx += 1
        return country_geocode

    @classmethod
    def regenerate_region_geocode(cls, nominatim_host, verbose=False):
        region_geocode = {}
        helper = CentroidUpdateHelper(nominatim_host)
        misses = set()
        hit, idx, tot = 0, 1, sum(map(len, REGION_MAPPING.values()))
        fmt = '%%0%dd' % int(ceil(log10(tot)))
        fmt = '%%s %s / %s :: %%s, %%s' % (fmt, fmt)
        s = time()
        for country_code, regions in REGION_MAPPING.iteritems():
            country_name = COUNTRY_MAPPING[country_code]
            region = {}
            for region_code, region_name in regions.iteritems():
                try:
                    region[region_code] = helper(country_name, region_name)
                    hit += 1
                    msg = 'Found'
                except:
                    misses.add((country_code, region_code))
                    region[region_code] = {}
                    msg = 'Miss '
                if verbose:
                    print fmt % (msg, idx, tot, country_name, region_name)
                idx += 1
            region_geocode[country_code] = region
        if verbose:
            print u"Region geocode rate of %.3f in %d seconds.\nMissed:" % (
                hit / float(tot), time() - s)
            misses = [cls.translate(*miss) for miss in misses]
            for country_code, region_code in sorted(misses):
                print u"%s, %s" % (country_code, region_code)
            print
        return region_geocode

    @classmethod
    def regenerate_phone_geocode(cls, nominatim_host, verbose=False):
        phone_geocode = {}
        helper = CentroidUpdateHelper(nominatim_host)
        hit, idx, tot = 0, 1, sum(
            (len(region) for areas in PHONE_MAPPING.itervalues()
             for region in areas.values()))
        fmt = '%%0%dd' % int(ceil(log10(tot)))
        fmt = '%%s %s / %s :: +%%03d-%%s :: %%s, %%s' % (fmt, fmt)
        s = time()

        for country_code, area_codes in PHONE_MAPPING.iteritems():
            phone_geocode[country_code] = {}
            for area_code, information in area_codes.iteritems():
                phone_geocode[country_code][area_code] = []
                for country_name, region_name in information:
                    try:
                        phone_geocode[country_code][area_code].append(
                            helper(country_name, region_name)['geojson'])
                        hit += 1
                        msg = 'Found Region '
                    except:
                        try:
                            phone_geocode[country_code][area_code].append(
                                helper(country_name)['geojson'])
                            hit += 1
                            msg = 'Found Country'
                        except:
                            msg = 'Miss         '
                    if verbose:
                        print fmt % (msg, idx, tot, country_code, area_code,
                                     country_name, region_name)
                    idx += 1
        if verbose:
            print u"Phone geocode rate of %.3f in %d seconds.\n" % (
                hit / float(tot), time() - s)
        return phone_geocode

    @classmethod
    def generate_full_geocode_set(cls, nominatim_host, verbose=False):
        from shapely.geometry import shape as Shape, mapping as Mapping
        from shapely.ops import cascaded_union

        country = cls.regenerate_country_geocode(nominatim_host, verbose)
        country = {
            code: {
                'geojson': Mapping(Shape(geo['geojson']).simplify(0.001)),
                'display_name': geo['display_name'],
                'namedetails': geo.get('namedetails', {})
            }
            for code, geo in country.iteritems()
            if geo.get('geojson') and geo.get('display_name')
        }
        with open('cgeo.json', 'wb') as out:
            dump(country, out)
        if exists('cgeo.json.xz'):
            if isfile('cgeo.json.xz'):
                remove('cgeo.json.xz')
            else:
                raise TypeError("cgeo.json.xz exists and is not a file!")
        Command('xz')('-z9', 'cgeo.json')

        regions = cls.regenerate_region_geocode(nominatim_host, verbose)
        regions = {
            ccode: {
                rcode: {
                    'geojson': Mapping(Shape(geo['geojson']).simplify(0.002)),
                    'display_name': geo['display_name'],
                    'namedetails': geo.get('namedetails', {})
                }
                for rcode, geo in region.iteritems()
                if geo.get('geojson') and geo.get('display_name')
            }
            for ccode, region in regions.iteritems()
        }
        with open('rgeo.json', 'wb') as out:
            dump(regions, out)
        if exists('rgeo.json.xz'):
            if isfile('rgeo.json.xz'):
                remove('rgeo.json.xz')
            else:
                raise TypeError("rgeo.json.xz exists and is not a file!")
        Command('xz')('-z9', 'rgeo.json')

        phones = cls.regenerate_phone_geocode(nominatim_host, verbose)
        phones = {
            ccode: {
                acode: cascaded_union(map(Shape, phones[ccode][acode])
                                      ).centroid.coords[0][::-1]
                for acode in phones[ccode]
            }
            for ccode in phones
        }
        with open('pgeo.json', 'wb') as out:
            dump(phones, out)
        if exists('pgeo.json.xz'):
            if isfile('pgeo.json.xz'):
                remove('pgeo.json.xz')
            else:
                raise TypeError("pgeo.json.xz exists and is not a file!")
        Command('xz')('-z9', 'pgeo.json')

    def get_country_shapes(self, country_geocode):
        country_shapes = {}
        for key in country_geocode:
            if country_geocode[key].get('geojson'):
                country_shapes[key] = create_from_geojson(
                    country_geocode[key]['geojson'])
        return country_shapes

    def get_country_fuzzies(self, country_geocode):
        country_fuzzies = FuzzyMapping()
        for ccode, details in country_geocode.iteritems():
            if details.get('display_name'):
                country_fuzzies[unicode(details['display_name'])] = ccode
            for name in details.get('namedetails', {}).itervalues():
                country_fuzzies[unicode(name)] = ccode
        return country_fuzzies

    @property
    def country_geocode(self):
        if self._country_geocode is None:
            self.prepare_countries(self.regenerate_country_geocode())
        return self._country_geocode

    def get_region_shapes(self, region_geocode):
        region_shapes = {}
        for ckey in region_geocode:
            region_shapes[ckey] = {}
            for rkey in region_geocode[ckey]:
                if region_geocode[ckey][rkey].get('geojson'):
                    region_shapes[ckey][rkey] = create_from_geojson(
                        region_geocode[ckey][rkey]['geojson'])
        return region_shapes

    def get_region_fuzzies(self, region_geocode):
        region_fuzzies = FuzzyMapping()
        for ccode, regions in region_geocode.iteritems():
            for rcode, details in regions.iteritems():
                if details.get('display_name'):
                    region_fuzzies[unicode(details['display_name'])
                                   ] = (ccode, rcode)
                for name in details.get('namedetails', {}).itervalues():
                    region_fuzzies[unicode(name)] = (ccode, rcode)
        return region_fuzzies

    @property
    def region_geocode(self):
        if self._region_geocode is None:
            self.prepare_regions(self.regenerate_region_geocode())
        return self._region_geocode

    def __init__(self, nominatim_host, _country_geocode=None,
                 _region_geocode=None, _phone_geocode=None,
                 _fuzzy_treshold=0.7, verbose=False):
        self.nominatim_host = nominatim_host

        ctime = time()
        if isinstance(_country_geocode, dict):
            _country_geocode = _country_geocode
        elif isinstance(_country_geocode, basestring):
            
            if _country_geocode.endswith('xz'):
                _country_geocode = loads(
                    Command('xz')('-dc', _country_geocode).stdout)
            else:
                with open(_country_geocode, 'rb') as cin:
                    _country_geocode = load(cin)
        elif isinstance(_country_geocode, Collection):
            _country_geocode = self.pull_from_mongo(_country_geocode)
        else:
            _country_geocode = None

        if _country_geocode is not None:
            self._country_geocode = self.get_country_shapes(_country_geocode)
            self._country_fuzzies = self.get_country_fuzzies(_country_geocode)
        else:
            self._country_geocode = None
        ctime = time() - ctime

        rtime = time()
        if isinstance(_region_geocode, dict):
            _region_geocode = _region_geocode
        elif isinstance(_region_geocode, basestring):
            if _region_geocode.endswith('xz'):
                _region_geocode = loads(
                    Command('xz')('-dc', _region_geocode).stdout)
            else:
                with open(_region_geocode, 'rb') as rin:
                    _region_geocode = load(rin)
        elif isinstance(_region_geocode, Collection):
            _region_geocode = self.pull_from_mongo(_region_geocode)
        else:
            _region_geocode = None

        if _region_geocode is not None:
            self._region_geocode = self.get_region_shapes(_region_geocode)
            self._region_fuzzies = self.get_region_fuzzies(_region_geocode)
        else:
            self._region_geocode = None
        rtime = time() - rtime

        ptime = time()
        if isinstance(_phone_geocode, dict):
            self._phone_geocode = _phone_geocode
        elif isinstance(_phone_geocode, basestring):
            if _phone_geocode.endswith('xz'):
                self._phone_geocode = loads(
                    Command('xz')('-dc', _phone_geocode).stdout)
            else:
                with open(_phone_geocode, 'rb') as pin:
                    self._phone_geocode = load(pin)
        elif isinstance(_phone_geocode, Collection):
            self._phone_geocode = self.pull_from_mongo(_phone_geocode)
        else:
            self._phone_geocode = None
        ptime = time() - ptime

        if isinstance(_fuzzy_treshold, float) and 0.0 < _fuzzy_treshold < 1.0:
            self._fuzzy_treshold = _fuzzy_treshold
        else:
            self._fuzzy_treshold = 0.7

        gtime = time()
        if self._country_geocode and self._region_geocode:
            self.__geo_tree = GeoTree()
            for ccode in self._country_geocode.iterkeys():
                for rcode, rpoly in self._region_geocode[ccode].iteritems():
                    self.__geo_tree.insert(
                        *centroid(rpoly), ccode=ccode, rcode=rcode)
        gtime = time() - gtime

        if verbose:
            print 'Country Polygon Time: %f seconds' % ctime
            print 'Region  Polygon Time: %f seconds' % rtime
            print 'Area Code Load  Time: %f seconds' % ptime
            print 'GeoTree Insert  Time: %f seconds' % gtime

    BASE_URL = '/search.php?country=%s'
    QUERY_URL = '/search.php?q=%s'
    BASE_ARGS = ('&format=json'
                 '&accept-language=en'
                 '&limit=1'
                 '&polygon_geojson=1'
                 '&addressdetails=1'
                 '&namedetails=1')

    def get_url(self, country, region=None, force_query=False):
        if force_query:
            if region:
                query = country + ', ' + region
            else:
                query = country
            query = self.nominatim_host + self.QUERY_URL % query
        else:
            query = self.nominatim_host + self.BASE_URL % country
            if region:
                query += '&state=' + region
        return query + self.BASE_ARGS

    def __call__(self, country, region=None, timeout=30.0, reattempt=5):
        for _ in range(reattempt):
            try:
                if u',' in country and region is None:
                    country_, region_ = country.split(u',', 1)
                    country_ = u' '.join(country_.strip().split())
                    region_ = u' '.join(region_.strip().split())
                else:
                    country_, region_ = country, region

                try:
                    url = self.get_url(country=country_, region=region_)
                    content = r_get(url, timeout=timeout).content
                    return loads(content.strip())[0]
                except IndexError:
                    pass
                try:
                    url = self.get_url(country=country_, region=region_,
                                       force_query=True)
                    content = r_get(url, timeout=timeout).content
                    return loads(content.strip())[0]
                except IndexError:
                    pass

                if region is None:
                    try:
                        url = self.get_url(country=country_)
                        content = r_get(url, timeout=timeout).content
                        return loads(content.strip())[0]
                    except IndexError:
                        pass
                    try:
                        url = self.get_url(country=country_, force_query=True)
                        content = r_get(url, timeout=timeout).content
                        return loads(content.strip())[0]
                    except IndexError:
                        pass
            except:
                print format_exc()
        raise LookupError("Failed to find results for query: %s, %s"
                          % (country, region))

    @staticmethod
    def translate(country, region=None):
        region = REGION_MAPPING.get(country, {}).get(region, None)
        country = COUNTRY_MAPPING.get(country, None)
        return country, region

    def codify(self, latitude=None, longitude=None, country_strings=(),
               region_strings=(), verbose=False, limit=100, **ignored):
        try:
            latitude = float(latitude)
            assert(-90.0 <= latitude <= 90.0)
        except:
            latitude = None
        try:
            longitude = float(longitude)
            assert(-180.0 <= longitude <= 180.0)
        except:
            longitude = None
        try:
            assert(isinstance(country_strings, Container) and
                   not isinstance(country_strings, basestring))
            country_strings = tuple(country for country in country_strings
                                    if isinstance(country, basestring))
        except:
            country_strings = ()
        try:
            assert(isinstance(region_strings, Container) and
                   not isinstance(region_strings, basestring))
            region_strings = tuple(region for region in region_strings
                                   if isinstance(region, basestring))
        except:
            region_strings = ()

        if verbose:
            if latitude and longitude:
                print u'Lat / Lon: %+08.3f / %+08.3f' % (latitude, longitude)
            else:
                print u'Lat / Lon:   None   /   None  '
            print u'Country Strings: %s' % u', '.join(country_strings)
            print u'Region  Strings: %s' % u', '.join(region_strings)

        country_code = None
        region_code = None

        if verbose:
            runtime = time()
        comparisons = 0

        if latitude is not None and longitude is not None:
            found = 0
            nnode = None
            point = Point(longitude, latitude)
            for node in self.__geo_tree.find_approximate_nearest(
                    latitude, longitude):
                found += 1

                ccode = node.ccode
                rcode = node.rcode

                country = self._country_geocode[ccode]
                region = self._region_geocode[ccode][rcode] if rcode else None

                if region:
                    comparisons += 1
                    if contains(region, point):
                        country_code = ccode
                        region_code = rcode
                        if verbose:
                            print u'GeoCode determined by intersection.'
                            print u'Time: %f seconds.' % (time() - runtime)
                            print u'Comp: %d polygons.' % comparisons
                            print self.translate(country_code, region_code)
                            print
                        break
                if nnode is None:
                    comparisons += 1
                    if contains(country, point):
                        nnode = node

                if found > limit:
                    break

            if country_code is None and nnode is not None:
                country_code = nnode.ccode
                region_code = nnode.rcode
                if verbose:
                    print u'GeoCode determined by proximity.'
                    print u'Time: %f seconds.' % (time() - runtime)
                    print u'Comp: %d polygons.' % comparisons
                    print self.translate(country_code, region_code)
                    print

        if (country_code is None and (
                any(country_strings) or any(region_strings))):
            best_conf = self._fuzzy_treshold
            for country in country_strings:
                comparisons += 1
                _, code, conf = self._country_fuzzies.get_stored_tuple(country)
                if conf > best_conf:
                    country_code, region_code = code, None
            for region in region_strings:
                comparisons += 1
                _, tup_, conf = self._region_fuzzies.get_stored_tuple(region)
                if conf > best_conf:
                    country_code, region_code = tup_
            if verbose:
                print u'GeoCode determined by fuzzy set comparison.'
                print u'Time: %f seconds.' % (time() - runtime)
                print u'Comp: %d polygons.' % comparisons
                print self.translate(country_code, region_code)
                print

        return country_code, region_code


if __name__ == '__main__':
    from ta_common.taste_config_helper import TasteConf
    nominatim_host = u'http://%s/nominatim/' % TasteConf().getNominatimHost()
    CentroidUpdateHelper.generate_full_geocode_set(nominatim_host, True)
