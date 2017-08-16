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

from collections import Container
from helpers import GeoTree
from json import load, loads, dump
from math import ceil, log10
from operator import itemgetter
from os import remove
from os.path import exists, isfile
from pymongo import CursorType
from pymongo.collection import Collection
from requests import get as r_get
from sh import Command
from ta_common.field_names import RO, MC, ENV
from ta_common.geo.mapping import (
    countries as COUNTRY_MAPPING, regions as REGION_MAPPING,
    phone_codes as PHONE_MAPPING, postal_codes as POSTAL_MAPPING)
from ta_common.geo.objects import (
    Point, create_from_geojson, centroid, contains, distance, union)
from ta_common.relational_object import mutabledotdict
from time import time
from traceback import format_exc

try:
    from cfuzzyset import cFuzzySet

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
except:
    FuzzyMapping = None


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
                'geojson': Mapping(Shape(geo['geojson']).simplify(0.005)),
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
                    'geojson': Mapping(Shape(geo['geojson']).simplify(0.01)),
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
        if FuzzyMapping is None:
            return None
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
        if FuzzyMapping is None:
            return None
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

        if ENV.get(ENV.STAGE, as_type=str).lower().startswith('china'):
            if self._country_geocode:
                for remove, replace in (
                        ("HKG", "CH06"), ("TWN", "CH27"), ("MAC", "CH34")):
                    poly = self._country_geocode.pop(remove, None)
                    self._country_geocode['CHN'] = union(
                        self._country_geocode['CHN'], poly)
                    if self._region_geocode:
                        _ = self._region_geocode.pop(remove, None)
                        self._region_geocode['CHN'][replace] = poly

        gtime = time()
        if self._country_geocode:
            self.__country_tree = GeoTree()
            for ccode, cpoly in self._country_geocode.iteritems():
                latitude, longitude = centroid(cpoly)
                self.__country_tree.insert(latitude, longitude, ccode=ccode)
        else:
            self.__country_tree = None

        if self._country_geocode and self._region_geocode:
            self.__region_trees = {}
            for ccode in self._country_geocode:
                if not self._region_geocode.get(ccode):
                    continue
                self.__region_trees[ccode] = GeoTree()
                for rcode, rpoly in self._region_geocode[ccode].iteritems():
                    latitude, longitude = centroid(rpoly)
                    self.__region_trees[ccode].insert(
                        latitude, longitude, ccode=ccode, rcode=rcode)
        else:
            self.__region_trees = {}
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
        region = REGION_MAPPING.get(country, {}).get(region, '<no region>')
        country = COUNTRY_MAPPING.get(country, '<no country>')
        return country, region

    def codify(self, latitude=None, longitude=None, country_strings=(),
               region_strings=(), verbose=False, limit=5, **ignored):
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
            if latitude is not None and longitude is not None:
                print u'Lat / Lon: %+08.3f / %+08.3f' % (latitude, longitude)
            else:
                print u'Lat / Lon:   None   /   None  '
            print u'Country Strings: %s' % u', '.join(country_strings)
            print u'Region  Strings: %s' % u', '.join(region_strings)

        country_code = None
        region_code = None

        if verbose:
            runtime = time()
        contains_calls = 0
        distance_calls = 0

        if (self.__country_tree is not None and
                latitude is not None and longitude is not None):
            ndist = nnode = None
            point = Point(longitude, latitude)
            for cnode in self.__country_tree.find_approximate_nearest(
                    latitude, longitude):

                ccode = cnode.ccode
                cpoly = self._country_geocode[ccode]

                contains_calls += 1
                if contains(cpoly, point):
                    if ccode in self.__region_trees:
                        ctree = self.__region_trees[ccode]
                        for rnode in ctree.find_approximate_nearest(
                                latitude, longitude):
                            if rnode.ccode != ccode:
                                continue
                            rcode = rnode.rcode

                            if rcode not in self._region_geocode[ccode]:
                                continue
                            rpoly = self._region_geocode[ccode][rcode]

                            contains_calls += 1
                            if contains(rpoly, point):
                                country_code = ccode
                                region_code = rcode
                                break

                        if region_code is None:
                            for rnode in self.__region_trees[ccode]:
                                rcode = rnode.rcode
                                rpoly = self._region_geocode[ccode][rcode]
                                distance_calls += 1
                                if nnode is None:
                                    ndist = distance(rpoly, point)
                                    nnode = rnode
                                else:
                                    rdist = distance(rpoly, point)
                                    if rdist < ndist:
                                        ndist = rdist
                                        nnode = rnode
                        else:
                            break
                    else:
                        country_code = ccode
                        region_code = None
                else:
                    distance_calls += 1
                    if nnode is None:
                        ndist = distance(cpoly, point)
                        nnode = cnode
                    else:
                        cdist = distance(cpoly, point)
                        if cdist < ndist:
                            ndist = cdist
                            nnode = cnode

                if country_code is not None:
                    if verbose:
                        print u'GeoCode determined by intersection.'
                        print u'Time: %f seconds.' % (time() - runtime)
                        print u'Cont: %d polygons.' % contains_calls
                        print u'Dist: %d polygons.' % distance_calls
                        print self.translate(country_code, region_code)
                        print
                    break

            if country_code is None and nnode is not None:
                country_code = nnode.ccode
                region_code = getattr(nnode, 'rcode', None)
                if verbose:
                    print u'GeoCode determined by proximity.'
                    print u'Time: %f seconds.' % (time() - runtime)
                    print u'Cont: %d polygons.' % contains_calls
                    print u'Dist: %d polygons.' % distance_calls
                    print self.translate(country_code, region_code)
                    print

        if (self._country_fuzzies is not None and
            self._region_fuzzies is not None and
            country_code is None and (
                any(country_strings) or any(region_strings))):
            best_conf = self._fuzzy_treshold
            for country in country_strings:
                _, code, conf = self._country_fuzzies.get_stored_tuple(country)
                if conf > best_conf:
                    country_code, region_code = code, None
            for region in region_strings:
                _, tup_, conf = self._region_fuzzies.get_stored_tuple(region)
                if conf > best_conf:
                    country_code, region_code = tup_
            if verbose:
                print u'GeoCode determined by fuzzy set comparison.'
                print u'Time: %f seconds.' % (time() - runtime)
                print u'Cont: %d polygons.' % contains_calls
                print u'Dist: %d polygons.' % distance_calls
                print self.translate(country_code, region_code)
                print

        return country_code, region_code

    def unittest(self, verbose=False, invalid=True):
        from ta_common.geo.centroid import (countries as UNIT_TEST_C,
                                            regions as UNIT_TEST_R)
        UNIT_TEST_MAP = {}
        for ccode in UNIT_TEST_C:
            if UNIT_TEST_R.get(ccode):
                UNIT_TEST_MAP[ccode] = UNIT_TEST_R[ccode]
            else:
                UNIT_TEST_MAP[ccode] = {None: UNIT_TEST_C[ccode]}

        start = time()
        score = total = 0
        missed = {}
        for valid_ccode in UNIT_TEST_MAP:
            for valid_rcode, (longitude, latitude) in (
                    UNIT_TEST_MAP[valid_ccode].iteritems()):
                total += 1
                found_ccode, found_rcode = self.codify(latitude, longitude)
                valid = ', '.join(self.translate(valid_ccode, valid_rcode))
                found = ', '.join(self.translate(found_ccode, found_rcode))

                if valid_ccode != found_ccode:
                    if invalid:
                        print("[C] [%9.3f] Invalid result for '%s' (Got '%s')."
                              % (time() - start, valid, found))
                    missed.setdefault(valid_ccode, []).append(found_ccode)
                elif valid_rcode != found_rcode:
                    if invalid:
                        print("[R] [%9.3f] Invalid result for '%s' (Got '%s')."
                              % (time() - start, valid, found))
                    missed.setdefault(valid_ccode, []).append(found_ccode)
                else:
                    if verbose:
                        print("[ ] [%9.3f] Valid result for %s."
                              % (time() - start, valid))
                    score += 1

        print("    [%9.3f] Test completed, success rate of %.3f%% (%d / %d)."
              % (time() - start, 100.0 * float(score) / total, score, total))
        if invalid:
            for ccode, confused in sorted(
                    missed.items(), key=lambda tup: len(tup[1])):
                country = self.translate(ccode)[0]

                confusion = {}
                for ocode in confused:
                    confusion[ocode] = confusion.get(ocode, 0) + 1

                confusion = ['%s: %d' % (self.translate(ocode)[0],
                                         confusion[ocode])
                             for ocode in sorted(confusion,
                                                 key=confusion.__getitem__,
                                                 reverse=True)]
                print("[%3d] %s => {%s}"
                      % (len(confused), country, ', '.join(confusion)))


if __name__ == '__main__':
    from ta_common.taste_config_helper import TasteConf
    nominatim_host = u'http://%s/nominatim/' % TasteConf().getNominatimURI()
    CentroidUpdateHelper.generate_full_geocode_set(nominatim_host, True)
