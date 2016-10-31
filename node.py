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

  @author: Christian Gibson, Thomas Kraft
  @contact: cgibson@tasteanalytics.com
  @project: Taste Analytics Computation Node-- Nominatim Request Handler

  @version: 0.25-sigma
  @updated: August 10, 2016
  @requires: python 2.7.10
"""

from cPickle import dumps as dump_obj, loads as load_obj
from collections import deque, Container, Counter, Mapping, OrderedDict
from geoip2.database import Reader
from geoip2.models import City
from jieba import cut as jieba_cut
from json import loads
from mapping import CentroidUpdateHelper, COUNTRY_MAPPING, REGION_MAPPING
from math import log10
from multiprocessing.pool import ThreadPool
from os import walk
from os.path import join
from phonenumbers import format_number, PhoneNumber, PhoneNumberMatcher
from pymongo import MongoClient, DESCENDING
from pymongo.cursor import CursorType
from regex import compile as regex_compile, UNICODE as regex_U
from requests import session, get as r_get
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, Timeout
from sys import stdout, maxunicode
from ta_common.field_names import DF, RO, MC, JS, ENV
from ta_common.geo.centroid import countries, regions, phones
from ta_common.geo.mapping import phone_codes
from ta_common.taste_config_helper import TasteConf
from threading import Lock
from time import time, sleep
from traceback import format_exc
from unicodedata import normalize
from urllib import urlencode
from urlparse import parse_qsl
from v2_ta_common.process_node import ProcessNode
from warnings import warn

TCONFIGURATION = TasteConf()
ACTIVATE_DEBUG = ENV.get(ENV.VERBOSE, as_type=int) > 1
CACHE_SIZE_CAP = 1000000
CACHE_SIZE_MIN = 10000
CODIFY_VERBOSE = ENV.get(ENV.VERBOSE, as_type=int) > 2
CONCURRENT_MAX = 128
INITIAL_PERIOD = 0.0
MAXIMUM_PERIOD = 3.0
MAXMIND_SOURCE = Reader(TCONFIGURATION.getMaxMindSource())
NOMINATIM_HOST = u'http://%s/nominatim/' % TCONFIGURATION.getNominatimHost()
REQUEST_PERIOD = INITIAL_PERIOD


def identity(obj):
    return obj


def gen_cache_key(params):
    return urlencode(sorted([(k.encode('utf-8'), v.encode('utf-8'))
                             for k, v in params.items()]))


def rest_cache_id(_id):
    return OrderedDict([(k.decode('utf-8'), v.decode('utf-8'))
                        for k, v in parse_qsl(_id)])


class GeocodeResult(object):
    COUNTRY_KEYS = ("country", "continent")
    REGION_KEYS = ("state", "city", "town", "county",
                   "state_district", "village", "suburb")

    MAXIMUM_ATTEMPTS = 5

    # The following rely on Nominatim queries
    STANDARD_ARGUMENTS = ("&polygon_geojson=0"
                          "&addressdetails=1"
                          # "&countrycodes=cn"  # Used for one-off jobs
                          # "&country=China"  # Used for one-off jobs
                          "&format=json")
    STANDARD_KWARGUMENTS = {
        "limit": lambda limit: "&limit={limit:d}" % min(max(int(limit), 1), 15)
    }

    TYPICAL_GEOCODE_QUERY = "geocode"
    TYPICAL_GEOCODE_SCRIPT = "search.php?"
    ATTEMPT_GEOCODE_ADJUST = regex_compile(r'[^\p{L}\p{N}\p{M},]', regex_U)
    CONSIDERATION_PRIORITY = ('country', 'state', 'county',
                              'city', 'street', 'postalcode')

    def run_geocode(self, query, errors):
        body = self.fxn(self.__ns + query + self.arguments)
        try:
            return loads(body.strip())
        except:
            if "DB Error" in body:
                self.__calls -= 1
                wait = REQUEST_PERIOD * 10
                if self.__debug:
                    stdout.write(
                        "\nDetected PostgreSQL database "
                        "error. Sleeping for %.2f seconds."
                        "\nBad Request: %s\n" % (wait, query))
                if "DB Error" in errors:
                    self.__calls += 1
                    errors.remove("DB Error")
                else:
                    errors.add("DB Error")
                sleep(wait)
            elif "Internal Server Error" in body:
                if self.__debug:
                    stdout.write(
                        "\nDetected Nominatim internal error."
                        "\nBad Request: %s\n" % query)
                sleep(REQUEST_PERIOD)
            else:
                if self.__debug:
                    stdout.write(
                        "\nEncountered unknown error.\n%s\n%s"
                        "\nBad Request: %s\n"
                        % (body, format_exc(), query))
                sleep(REQUEST_PERIOD)
            raise

    @classmethod
    def get_geocode(cls, query, attempt=0, juggle=False):
        params = {field: query[field] for field in cls.CONSIDERATION_PRIORITY
                  if field in query and query[field]}
        if params:
            for idx in range(len(cls.CONSIDERATION_PRIORITY)):
                if attempt > 0:
                    field = cls.CONSIDERATION_PRIORITY[-(idx + 1)]
                    if field in params:
                        del params[field]
                        attempt -= 1
                else:
                    if params:
                        return params
                    else:
                        break

        if attempt > 0:
            if juggle:
                attempt += 1
                if attempt % 2:
                    cut = slice(int(attempt // 2), None, None)
                else:
                    cut = slice(None, -int(attempt // 2), None)
            else:
                cut = slice(int(attempt), None, None)
        else:
            cut = slice(None, None, None)

        if 'q' not in query or not query['q'].strip():
            query['q'] = u', '.join(
                query[field].strip() for field in cls.CONSIDERATION_PRIORITY
                if field in query and query[field].strip())
        query_input = query['q']

        if u',' in query_input:
            split_input = u','.join(query_input.split(u',')[cut]).split()
        else:
            split_input = cls.ATTEMPT_GEOCODE_ADJUST.split(query_input)[cut]

        if split_input:
            return {"q": u' '.join(split_input).rstrip(u',')}
        else:
            return None

    def res_geocode(self, query, errors):
        return self.run_geocode(
            self.TYPICAL_GEOCODE_SCRIPT + gen_cache_key(query), errors)

    REVERSE_GEOCODE_QUERY = "reverse"
    REVERSE_GEOCODE_SCRIPT = "reverse.php?"

    @classmethod
    def get_reverse(cls, query, attempt=0):
        # For an explanation of recorded accuracy see:
        # https://en.wikipedia.org/wiki/Decimal_degrees#Precision
        if attempt == 0:
            return {"lat": query["lat"], "lon": query["lon"]}
        else:
            return None, None

    def res_reverse(self, query, errors):
        return self.run_geocode(
            self.REVERSE_GEOCODE_SCRIPT + gen_cache_key(query), errors)

    # Phone number lookup currently provided by ad-hoc area code guesswork
    PHONE_NUMBER_QUERY = "phone_number"

    @staticmethod
    def _phone_numbers(query, region=None):
        if not isinstance(query, basestring):
            raise StopIteration
        found = False
        for guess in (region, "US", "CN", "RU"):
            try:
                for match in PhoneNumberMatcher(query, region=guess):
                    yield format_number(match.number, 1)
                    found = True
            except:
                pass
            if found:
                break

    def get_phone_number(self, query, attempt=0):
        if not hasattr(self, '_numbers'):
            self._numbers = self._phone_numbers(query[self.PHONE_NUMBER_QUERY])
            self._matched = []

        while attempt >= len(self._matched):
            try:
                self._matched.append(self._numbers.next())
            except StopIteration:
                self._matched.append(None)

        try:
            return self._matched[attempt]
        except:
            return None

    def res_phone_number(self, query, errors):
        code, area_str = query.split()
        code = int(code.lstrip('+'))

        areas = phones[code]
        min_area = int(log10(min(filter(None, areas))) + 1)
        max_area = int(log10(max(filter(None, areas))) + 2)

        area = None
        for idx in xrange(min_area, max_area):
            area = int(area_str[:idx])
            if area in areas:
                break
            area = None

        lat, lon = areas[area]
        loc = phone_codes[code][area]

        try:
            if len(loc) > 1:
                country = Counter(l[0] for l in loc).most_common(1)[0][0]
                city = ''
            else:
                country, city = loc[0]
        except:
            country, city = '', ''

        return {
            "lat": lat,
            "lon": lon,
            "display_name": query,
            "address": {
                "country": country,
                "city": city
            }
        }

    # IP address lookup provided by an internal MaxMind database
    IP_ADDRESS_QUERY = "ip_address"

    def get_ip_address(self, query, attempt=0):
        try:
            return query[self.IP_ADDRESS_QUERY][attempt]
        except:
            return None

    def res_ip_address(self, query, errors):
        try:
            result = MAXMIND_SOURCE.city(query)
            if not isinstance(result, City):
                return None
        except:
            return None

        city = getattr(result, 'city', None)
        _id = getattr(city, 'geoname_id', -1)

        loc = getattr(result, 'location', None)
        lat = getattr(loc, 'latitude', None)
        lon = getattr(loc, 'longitude', None)
        post = getattr(loc, 'postal_code', None)

        city = getattr(city, 'name', '')
        country = getattr(getattr(result, 'country', None), 'name', '')
        postal = getattr(getattr(result, 'postal', None), 'code', None)
        traits = getattr(result, 'traits', None)
        ip_address = getattr(traits, 'ip_address', query)

        return {
            "osm_id": _id,
            "lat": lat,
            "lon": lon,
            "postcode": post,
            "display_name": ip_address,
            "address": {
                "country": country,
                "city": city,
                "postcode": postal
            }
        }

    @staticmethod
    def default_fetch_function(url, **kwargs):
        return r_get(url, **kwargs).content

    def __init__(self, host, query, assoc, search_type=TYPICAL_GEOCODE_QUERY,
                 _fetch_function=default_fetch_function, _catch_exceptions=(),
                 **kwargs):
        self.__ns = host
        self.__key = None
        self.__type = search_type
        self.__assoc = assoc
        self.__cache = kwargs.pop('cache', None)
        self.__calls = 0
        if hasattr(_fetch_function, '__call__'):
            self.fxn = _fetch_function
        else:
            self.fxn = self.default_fetch_function
        if hasattr(_catch_exceptions, '__iter__'):
            self.exc = tuple(exception for exception in _catch_exceptions
                             if (isinstance(exception, type) and
                                 issubclass(exception, Exception)))
        else:
            self.exc = tuple()
        self.__query = query
        self.get_query = getattr(self, 'get_%s' % self.__type)
        self.res_query = getattr(self, 'res_%s' % self.__type)
        self.arguments = self.STANDARD_ARGUMENTS

        self.__cached = False
        self.__result = None

        for kwarg in kwargs:
            call = self.STANDARD_KWARGUMENTS.get(kwarg, None)
            if isinstance(call, basestring):
                self.arguments += call % kwarg
            elif hasattr(call, '__call__'):
                self.arguments += call(kwarg)

    @property
    def calls(self):
        if self.__result is None:
            self.result
        return self.__calls

    @property
    def call_was_cached(self):
        if self.__result is None:
            self.result
        return self.__cached

    @property
    def cache_key(self):
        if self.__result is None:
            self.result
        return self.__key

    @property
    def result(self):
        key = ''
        errors = set()
        mirror = set()

        if self.__result is None:
            self.__result = []

            while not self.__result and self.__calls < self.MAXIMUM_ATTEMPTS:
                global REQUEST_PERIOD
                try:
                    query = self.get_query(self.__query, self.__calls)
                    if query is None:
                        self.__result = []
                        break

                    if self.__cache:
                        if isinstance(query, dict):
                            key = gen_cache_key(query)
                        else:
                            key = gen_cache_key({self.__type: query})
                        if key in self.__cache:
                            if ACTIVATE_DEBUG:
                                stdout.write("Query #%d: '%s' (Cached)\n"
                                             % (self.__calls + 1, query))
                            self.__cached = True
                            self.__result = self.__cache[key]
                            break

                    if ACTIVATE_DEBUG:
                        stdout.write("Query #%d: '%s'\n"
                                     % (self.__calls + 1, query))

                    try:
                        self.__calls += 1
                        docs = self.res_query(query, errors)
                        REQUEST_PERIOD = max(INITIAL_PERIOD,
                                             REQUEST_PERIOD * 0.99)
                    except self.exc as err:
                        self.__calls -= 1
                        docs = None
                        if type(err) in errors:
                            self.__calls += 1
                            errors.remove(type(err))
                        else:
                            errors.add(type(err))
                        if ACTIVATE_DEBUG:
                            stdout.write("\nEncountered expected %s error!\n%s"
                                         % (self.__type, format_exc()))
                        sleep(REQUEST_PERIOD)
                    except:
                        REQUEST_PERIOD = min(MAXIMUM_PERIOD,
                                             REQUEST_PERIOD * 1.05)
                        if ACTIVATE_DEBUG:
                            stdout.write("\nEncountered unknown %s error!\n%s"
                                         % (self.__type, format_exc()))
                        docs = None
                    if docs:
                        if not isinstance(docs, list):
                            docs = [docs]
                        if self.__cache and key:
                            self.__key = key
                        self.__result = docs
                        break
                    else:
                        if self.__cache:
                            mirror.add(key)
                except:
                    self.__calls += 1
                    if ACTIVATE_DEBUG:
                        stdout.write("\nEncountered unknown global error!\n%s"
                                     % format_exc())
                    sleep(REQUEST_PERIOD)

        try:
            if not self.__cached:
                self.__result = map(self.__normalize, self.__result)
                if self.__cache:
                    self.__cache[key] = self.__result
                    for alt in mirror:
                        self.__cache.register_alternate(key, alt)
        except:
            if ACTIVATE_DEBUG:
                stdout.write("\nEncountered issue with result normalization!"
                             "\n%s" % format_exc())
            self.__result = []

        if isinstance(self.__result, list) and len(self.__result) > 0:
            return self.__result
        else:
            return None

    def __normalize(self, dictionary):
        dictionary = dictionary if isinstance(dictionary, dict) else dict()
        address = dictionary.get("address", {})
        country_args = tuple(address[key] for key in self.COUNTRY_KEYS
                             if key in address and address[key])
        region_args = tuple(address[key] for key in self.REGION_KEYS
                            if key in address and address[key])

        result = {
            "id": int(dictionary.get("osm_id", -1)),
            "full": dictionary.get("display_name", ""),
        }

        try:
            latitude = float(dictionary["lat"])
            longitude = float(dictionary["lon"])
            result.update(
                {
                    "json": dictionary.get("geojson", {
                        "type": "Point",
                        "coordinates": [
                            longitude, latitude
                        ]
                    })
                }
            )
        except:
            latitude = None
            longitude = None

        try:
            country_code, region_code = self.__assoc.codify(
                latitude=latitude, longitude=longitude, verbose=CODIFY_VERBOSE,
                country_strings=country_args, region_strings=region_args)

            country_code = country_code.upper()
            region_code = region_code.upper() if region_code else None
            if not region_code or any(country_args) and not any(region_args):
                longitude, latitude = countries.get(
                    country_code, (longitude, latitude))
                region_code = None
            else:
                longitude, latitude = regions.get(country_code, {}).get(
                    region_code, (longitude, latitude))

            result.update(
                {
                    "lat": latitude,
                    "lon": longitude,
                    "code": {
                        "country": country_code,
                        "country_name": COUNTRY_MAPPING.get(country_code, None),

                        "region": region_code if region_code else None,
                        "region_name": REGION_MAPPING.get(
                            country_code, {}).get(region_code, None),

                        "postal": address.get("postcode", None)
                    }
                }
            )
        except:
            result.update(
                {
                    "lat": latitude,
                    "lon": longitude,
                    "code": {
                        "country": None,
                        "country_name": None,

                        "region": None,
                        "region_name": None,

                        "postal": address.get("postcode", None)
                    },
                }
            )

        return result


class LockedIterator(object):

    def __init__(self, iterator, lock_past):
        self.__iter = (obj for obj in iterator)
        self.__seen = 0
        self.__past = lock_past
        self.__lock = Lock()

    def __add__(self, other):
        self.__seen += other
        return self

    def __sub__(self, other):
        self.__seen -= min(other, self.__seen)
        if self.__seen <= self.__past:
            self.unlock()
        return self

    def __len__(self):
        return max(self.__seen, 0)

    def __iter__(self):
        while True:
            try:
                yield self.__next__()
            except:
                break

    def __next__(self):
        if self.__seen > self.__past:
            self.__lock.acquire()
            return self.__next__()
        else:
            self.__seen += 1
            return next(self.__iter)

    def __repr__(self):
        return '<iterlock %d / %d %slocked>' % (
            self.__seen, self.__past, '' if self.is_locked() else 'un')

    def next(self):
        return self.__next__()

    def __internal_state(self, open=False, lock=False):
        """
          Helper method which returns the internal state of the lock:
            True, if the lock is currently locked
            False, if the lock is currently opened

          This method can also be used to change the internal lock state:
            Given open=True, the lock will be opened
            Given lock=True, the lock will be closed
            Given open=False and lock=False, the lock's state will be
              determined and returned, after restoring the lock's state.

          See below for a functional explanation.

          The function threading.Lock.acquire takes a single positional
            argument, which defaults to True. This boolean flag allows the user
            to switch between two methods of Lock acquisition:

          threading.Lock.acquire(True) --> Block until the lock is acquired.
          threading.Lock.acquire(False) --> Attempt to acquire lock.
            If the lock is *already* acquired, return False,
            Else, acquire the lock, and return True.

          As such, method logic proceeds as follows:
          Situation A:
            threading.Lock.acquire(False) returns False,
            The lock was in a locked state, and still is.
          Situation B:
            threading.Lock.acquire(False) returns True,
            The lock was in an opened state, and is now locked.
          Regardless of situation, the lock is now locked. If the keyword
            argument "open" was passed in as True, we open the lock, and report
            the internal state as opened (False).
          Likewise, if "lock" was passed as True: we perform no further
            modifications to the lock itself, and report the internal state as
            locked (True).
          Otherwise, if the lock was originally open, it is now locked. We
            release the lock, then return the boolean flag originally received
            from threading.Lock.acquire(False).
        """
        was_open = self.__lock.acquire(False)
        if open:
            self.__lock.release()
            return False
        elif lock:
            return True
        elif was_open:
            self.__lock.release()
        return was_open

    def is_locked(self):
        return self.__internal_state()

    def unlock(self):
        return self.__internal_state(open=True)


class Nominatim(object):

    OPTION_PRIORITY_ORDER = (
        ("phone_number", GeocodeResult.PHONE_NUMBER_QUERY),
        ("ip_address", GeocodeResult.IP_ADDRESS_QUERY),
        ("latitude_longitude", GeocodeResult.REVERSE_GEOCODE_QUERY),
        ("longitude_latitude", GeocodeResult.REVERSE_GEOCODE_QUERY),
        ("latitude", GeocodeResult.REVERSE_GEOCODE_QUERY),
        ("global", GeocodeResult.TYPICAL_GEOCODE_QUERY),
        ("subglobal", GeocodeResult.TYPICAL_GEOCODE_QUERY),
        ("local", GeocodeResult.TYPICAL_GEOCODE_QUERY),
        ("sublocal", GeocodeResult.TYPICAL_GEOCODE_QUERY),
        ("postcode", GeocodeResult.TYPICAL_GEOCODE_QUERY),
    )

    LEGAL_CONFIGURATION_OPTIONS = tuple(
        option for option, _ in OPTION_PRIORITY_ORDER
    ) + ("longitude", "unknown")

    @classmethod
    def spawn_for_local(cls, directory='/', client=Ellipsis,
                        nominatim_host='http://nominatim.openstreetmap.org/',
                        **kwargs):
        country_geocode = None
        region_geocode = None
        phone_geocode = None
        for dirname, _dirpath, filenames in walk(directory):
            if country_geocode is None and 'cgeo.json.xz' in filenames:
                country_geocode = join(dirname, 'cgeo.json.xz')
            if region_geocode is None and 'rgeo.json.xz' in filenames:
                region_geocode = join(dirname, 'rgeo.json.xz')
            if phone_geocode is None and 'pgeo.json.xz' in filenames:
                phone_geocode = join(dirname, 'pgeo.json.xz')
            if country_geocode and region_geocode and phone_geocode:
                break

        if country_geocode and region_geocode:
            configuration = {'_country_geocode': country_geocode,
                             '_region_geocode': region_geocode,
                             'verbose': CODIFY_VERBOSE}
            if phone_geocode:
                configuration['_phone_geocode'] = phone_geocode
            kwargs.update(configuration)
            return cls(client, nominatim_host, **kwargs)

    @classmethod
    def generate_field_mapping(cls, config):
        mapping = {}

        try:
            gindex = config.params.geo_index
        except:
            gindex = {}

        for index_type in cls.LEGAL_CONFIGURATION_OPTIONS:
            for index_field in gindex.get(index_type, ()):
                mapping[index_field] = index_type

        return mapping

    NS = regex_compile(r'[\p{script=Han}\p{script=Tibetan}\p{script=Lao}'
                       r'\p{script=Thai}\p{script=Khmer}]', regex_U)
    NS = frozenset(NS.findall(u''.join(unichr(i) for i in xrange(maxunicode))))
    # Hardcoded geo-string replacement values.
    HC = {
        # u'\u5176\u4ed6': u'\u5176\u4ed6\u7701',
        # u'\u56db\u5ddd': u'\u56db\u5ddd\u7701',
        # u'\u5c71\u4e1c': u'\u5c71\u4e1c\u7701',
        # u'\u5c71\u897f': u'\u5c71\u897f\u7701',
        # u'\u5e7f\u4e1c': u'\u5e7f\u4e1c\u7701',
        # u'\u6c5f\u897f': u'\u6c5f\u897f\u7701',
        # u'\u6cb3\u5317': u'\u6cb3\u5317\u7701',
        # u'\u6cb3\u5357': u'\u6cb3\u5357\u7701',
        # u'\u6d77\u5916': u'\u6d77\u5916\u7701',
        # u'\u9655\u897f': u'\u9655\u897f\u7701',
        # u'\u9752\u6d77': u'\u9752\u6d77\u7701',
    }

    def remap_documents(self, document, mapping):
        information = {field_type: [] for field_type in mapping.values()}
        for field_name, field_type in mapping.iteritems():
            information[field_type].append(document.get(field_name))
        for field_type in information.iterkeys():
            information[field_type] = normalize('NFKC', u' '.join(
                filter(None, (p.strip() for p in information[field_type]))))
            if self.NS.intersection(information[field_type]):
                information[field_type] = u' '.join(
                    part.strip() for part in jieba_cut(information[field_type]))
            information[field_type] = self.HC.get(
                information[field_type], information[field_type])

        for field_type, search_type in self.OPTION_PRIORITY_ORDER:
            geo_lookup = getattr(self, '_' + field_type)(information)
            if isinstance(geo_lookup, dict):
                return search_type, geo_lookup
        return GeocodeResult.TYPICAL_GEOCODE_QUERY, self._unknown(information)

    LL = regex_compile(r'[^\+\-\.0-9]+')

    def _latitude_longitude(self, information):
        try:
            information['latitude'], information['longitude'] = map(
                float, filter(None, self.LL.split(
                    information['latitude_longitude']))[:2])
            return self._latitude(information)
        except:
            return None

    def _longitude_latitude(self, information):
        try:
            information['longitude'], information['latitude'] = map(
                float, filter(None, self.LL.split(
                    information['longitude_latitude']))[:2])
            return self._latitude(information)
        except:
            return None

    def _latitude(self, information):
        try:
            lat = '%010.5f' % float(information['latitude'])
            lon = '%010.5f' % float(information['longitude'])
            return {'lat': lat, 'lon': lon,
                    'orig': '%s, %s' % (lat, lon)}
        except:
            return None

    IA = regex_compile(r'[^0-9a-fA-F\:\.]', regex_U)

    def _ip_address(self, information):
        try:
            addresses = filter(
                None, sum([self.IA.split(ia)
                           for ia in information['ip_address'].split()], []))
            return {'ip_address': addresses,
                    'orig': ' / '.join(addresses)}
        except:
            return None

    def _phone_number(self, information):
        try:
            return {'phone_number': information['phone_number'].split()}
        except:
            return None

    def __clean_carry_and_extra(self, carry=None, extra=None):
        if isinstance(carry, Mapping):
            carry = dict(carry)
        else:
            carry = {}

        if isinstance(extra, basestring):
            extra = (extra,)
        elif isinstance(extra, Container):
            extra = tuple(extra)
        else:
            extra = tuple()

        return carry, extra

    CN = regex_compile(r'([\p{L}\p{N}]\P{Z}*[\p{L}\p{N}])', regex_U)

    def _global(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            global_ = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('global', u'')))
            if global_:
                carry['country'] = global_
                extra = (global_,) + extra
            return self._subglobal(information, carry, extra)
        except:
            return None

    def _subglobal(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            subglobal_ = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('subglobal', u'')))
            if subglobal_:
                carry['state'] = subglobal_
                extra = (subglobal_,) + extra
            return self._local(information, carry, extra)
        except:
            return None

    def _local(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            local_ = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('local', u'')))
            if local_:
                carry['city'] = local_
                extra = (local_,) + extra
            return self._sublocal(information, carry, extra)
        except:
            return None

    def _sublocal(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            sublocal_ = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('sublocal', u'')))
            if sublocal_:
                carry['street'] = sublocal_
                extra = (sublocal_,) + extra
            return self._postcode(information, carry, extra)
        except:
            return None

    def _postcode(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            postcode = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('postcode', u'')))
            if postcode:
                carry['postalcode'] = postcode
                extra = (postcode,) + extra
            return self._unknown(information, carry, extra)
        except:
            return None

    def _unknown(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            unknown = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('unknown', u'')))
            if unknown:
                extra = (unknown,) + extra
            if extra:
                carry['q'] = u', '.join(
                    OrderedDict.fromkeys(filter(None, extra)))
                carry['orig'] = carry['q']
            if any(carry.values()):
                return carry
            else:
                return None
        except:
            return None

    def __init__(self, client, nominatim_host=NOMINATIM_HOST, **kwargs):
        if isinstance(client, MongoClient):
            self.__client = client
        elif client is Ellipsis:
            warn("Ignoring invalid client -- cannot run jobs!", RuntimeWarning)
        else:
            raise TypeError("Nominatim must be started with a MongoClient!")

        self.__ns = kwargs['nominatim_host'] = nominatim_host
        self.__map = CentroidUpdateHelper(**kwargs)
        self.__used = set()
        self.__cache = CacheDictionary(maxsize=CACHE_SIZE_CAP, weakref=False)
        self.__session = session()
        self.__session.mount(
            prefix=self.__ns,
            adapter=HTTPAdapter(pool_connections=CONCURRENT_MAX,
                                pool_maxsize=CONCURRENT_MAX * 2,
                                pool_block=False))

    def session_fetch_function(self, url, **kwargs):
        return self.__session.get(url, timeout=30.0, **kwargs).content

    def restore_from_cache(self, subdomain, limit=None):
        if isinstance(limit, int):
            limit = int(max(min(limit, self.__cache.maxsize), CACHE_SIZE_MIN))
        else:
            limit = self.__cache.maxsize

        self.__cache = CacheDictionary(maxsize=self.__cache.maxsize,
                                       weakref=False)
        for doc in self.__client[subdomain][MC.CACHE_COL].find().sort(
                RO.LAST, DESCENDING).limit(limit):
            if 'value' in doc and doc['value']:
                search_query = doc.pop(RO.OBJECT_ID)
                self.__cache[gen_cache_key(search_query)] = doc['value']

    def update_mongo_cache(self, subdomain):
        action = time() * 1000.0
        bulk = self.__client[
            subdomain][MC.CACHE_COL].initialize_unordered_bulk_op()
        for key in self.__used:
            if not key:
                continue
            _id = rest_cache_id(key)
            val = self.__cache.quiet_get(key)
            if val:
                bulk.find({RO.OBJECT_ID: _id}).upsert().update(
                    {'$set': {'value': val, RO.LAST: action}})
        bulk.execute()

    def process(self, config, subdomain=None, pool_size=4, verbose=False):
        for _ in self.iterprocess(config, subdomain, pool_size, verbose):
            pass

    def iterprocess(self, config, subdomain=None, pool_size=4, verbose=False):
        if verbose:
            runtime = time()
        self.HITS = 0
        self.MISS = 0
        self.CONT = 0
        self.CODE = 0
        self.FUZZ = 0
        self.NULL = 0

        subdomain = config['mongo_db'] if subdomain is None else subdomain
        if subdomain is Ellipsis:
            pass
        elif subdomain:
            if verbose:
                stdout.write("[% 9.3f] Beginning retrieval of mongo cache...\n"
                             % (time() - runtime))
            self.restore_from_cache(subdomain=subdomain,
                                    limit=config.meta.counts.total)
            if verbose:
                stdout.write("[% 9.3f] Retrieval of mongo cache complete.\n"
                             % (time() - runtime))

        self._config = self.generate_field_mapping(config)
        self.__pool = ThreadPool(min(max(int(pool_size), 0), CONCURRENT_MAX))

        bulk = self.__client[config.mongo_db][
            config.mongo_table].initialize_unordered_bulk_op()
        locked = LockedIterator(
            self.__client[config.mongo_db][config.mongo_table].find(
                {}, projection={field: 1 for field in self._config},
                cursor_type=CursorType.EXHAUST),
            lock_past=CONCURRENT_MAX * 150)

        if verbose:
            stdout.write("[% 9.3f] %d hits / %d misses / %d calls <--> "
                         "%d coded (nom) / %d coded (idf) / %d empty <--> "
                         "%04.2f sleep / %03.2f codes / %d total ~~ "
                         "%d in iterlock.\r" % (
                             time() - runtime,

                             self.HITS,
                             self.MISS,
                             self.MISS + self.CONT,

                             self.CODE,
                             self.FUZZ,
                             self.NULL,

                             REQUEST_PERIOD,
                             float(self.HITS + self.MISS) / (time() - runtime),
                             self.HITS + self.MISS,

                             len(locked)))
            stdout.flush()
            last = time()

        for _id, geo in self.__pool.imap_unordered(self.__process, locked):
            locked -= 1
            if verbose and (time() - last) > 0.5:
                stdout.write(
                    "[% 9.3f] %d hits / %d misses / %d calls <--> "
                    "%d coded (nom) / %d coded (idf) / %d empty <--> "
                    "%04.2f sleep / %03.2f codes / %d total ~~ "
                    "%d in iterlock.\r" % (
                        time() - runtime,

                        self.HITS,
                        self.MISS,
                        self.MISS + self.CONT,

                        self.CODE,
                        self.FUZZ,
                        self.NULL,

                        REQUEST_PERIOD,
                        float(self.HITS + self.MISS) / (time() - runtime),
                        self.HITS + self.MISS,

                        len(locked)
                    ))
                stdout.flush()
                last = time()
            if not _id:
                continue
            bulk.find({RO.OBJECT_ID: _id}).update_one({'$set': {DF.geo: geo}})
            yield geo

        if verbose:
            stdout.write(
                "[% 9.3f] %d hits / %d misses / %d calls <--> "
                "%d coded (nom) / %d coded (idf) / %d empty <--> "
                "%04.2f sleep / %03.2f codes / %d total ~~ "
                "%d in iterlock.\n" % (
                    time() - runtime,

                    self.HITS,
                    self.MISS,
                    self.MISS + self.CONT,

                    self.CODE,
                    self.FUZZ,
                    self.NULL,

                    REQUEST_PERIOD,
                    float(self.HITS + self.MISS) / (time() - runtime),
                    self.HITS + self.MISS,

                    len(locked)
                ))
            stdout.flush()
            stdout.write("[% 9.3f] Geocoding complete.\n"
                         % (time() - runtime))

        self.__pool.close()
        self.__pool.join()

        if verbose:
            stdout.flush()
            stdout.write("[% 9.3f] Subthreads joined.\n"
                         % (time() - runtime))

        bulk.execute()
        if verbose:
            stdout.write("[% 9.3f] Bulk insertion of results complete.\n"
                         % (time() - runtime))

        if subdomain is Ellipsis:
            pass
        elif subdomain:
            self.update_mongo_cache(subdomain=subdomain)
            if verbose:
                stdout.write("[% 9.3f] Bulk update of mongo cache complete.\n"
                             % (time() - runtime))

    def get_geocode_result(self, type_, query):
        return GeocodeResult(
            host=self.__ns, query=query, assoc=self.__map, cache=self.__cache,
            search_type=type_, _fetch_function=self.session_fetch_function,
            _catch_exceptions=(ConnectionError, Timeout))

    def __process(self, dictionary):
        try:
            _id = dictionary[RO.OBJECT_ID]

            type_, query = self.remap_documents(dictionary, self._config)
            if isinstance(query, dict):
                geocode = self.get_geocode_result(type_, query)

                if geocode.call_was_cached:
                    self.HITS += 1
                else:
                    self.MISS += 1
                    self.CONT += geocode.calls - 1

                if geocode.result:
                    result = geocode.result[0]
                    self.__used.add(geocode.cache_key)
                else:
                    result = {}

                if result.get('full'):
                    self.CODE += 1

                if result.get('code', {}).get('country'):
                    self.FUZZ += 1
                else:
                    self.NULL += 1

                result['orig'] = query.get('orig', '')
            else:
                result = {}

            return _id, result
        except:
            return None, {}


class CacheDictionary(object):

    def __init__(self, maxsize=CACHE_SIZE_MIN, weakref=False):
        if maxsize < 1:
            raise ValueError("Cannot instantiate a dictionary with a zero or "
                             "negative key count!")
        super(self.__class__, self).__init__()
        self.__cache = dict()
        self.__keys = deque(maxlen=maxsize)
        self.__ref = weakref
        self.__reg = dict()
        self.__size = maxsize
        self.__store = maxsize

    @property
    def raw(self):
        return self.__cache

    @property
    def maxsize(self):
        return self.__store

    def __contains__(self, key):
        return self.__reg.get(key, key) in self.__cache

    def __iter__(self):
        return self.__cache.__iter__()

    def quiet_get(self, key):
        val = self.__cache.__getitem__(self.__reg.get(key, key))
        if self.__ref:
            return val
        else:
            return load_obj(dump_obj(val, True))

    def __getitem__(self, key):
        val = self.quiet_get(key)
        self.__prioritize(key)
        return val

    def register_alternate(self, key, alt):
        self.__reg[alt] = key

    def __setitem__(self, key, val):
        if key in self.__cache:
            adj = 0
        else:
            adj = 1
        if self.__ref:
            self.__cache.__setitem__(key, val)
        else:
            self.__cache.__setitem__(key, load_obj(dump_obj(val, True)))
        self.__size -= adj
        if self.__size < 0:
            self.__delete_oldest()
        self.__prioritize(key)

    def __delitem__(self, key):
        self.__cache.__delitem__(key)
        self.__keys.remove(key)
        purge = []
        for alt in self.__reg:
            if self.__reg[alt] == key:
                purge.append(alt)
        for alt in purge:
            del self.__reg[alt]
        self.__size += 1

    def __prioritize(self, key):
        try:
            self.__keys.remove(key)
        except ValueError:
            pass
        self.__keys.append(key)

    def __delete_oldest(self):
        self.__delitem__(self.__keys[0])


class GeoLookup(ProcessNode):
    processed_docs = 0
    update_meta = False
    update_conf = False

    def get_stage_code(self):
        return JS.GEO_CODE  # @UndefinedVariable

    def get_stage_detail(self):
        return JS.GEO_STATUS  # @UndefinedVariable

    @property
    def node_blocks(self):
        return False

    def setup(self):
        print "Starting with Nominatim host:", NOMINATIM_HOST
        init_start = time()
        self.nominatim = Nominatim(
            client=self.mongo_helper,
            _country_geocode=TCONFIGURATION.getNominatimCountryGeoJSON(),
            _region_geocode=TCONFIGURATION.getNominatimRegionGeoJSON())
        print "Nominatim setup required %.3f seconds." % (time() - init_start)

    def define_aws_queues(self):
        return [self.taste_conf.getTier3GeoSQSQueue(),
                self.taste_conf.getTier3HoldNodeQueue(), ]

    def aws_input_queue(self):
        return self.taste_conf.getTier3GeoSQSQueue()

    def aws_output_queue(self):
        return [self.taste_conf.getTier3HoldNodeQueue()]

    def node_timeout_seconds(self):
        # Allow three minutes, plus one second per 10 documents.
        # This is a worst-case consideration; assumes each document is unique.
        return 180 + self.job_configuration.meta.counts.total * 0.1

    def run_process(self):
        if self.job_configuration.params.geo_index:
            for _ in self.nominatim.iterprocess(
                    config=self.job_configuration,
                    subdomain=self.job_configuration.mongo_db,
                    verbose=ENV.get(ENV.VERBOSE, as_type=int)):
                self.processed_docs += 1

    def state(self):
        return {'processed_docs': self.processed_docs}

    def finish_job(self):
        pass

if __name__ == '__main__':
    def get_mapping(nominatim, config, grove):
        return nominatim.generate_field_mapping(config)

    def get_example(nominatim, config, grove):
        mapping = get_mapping(nominatim, config, grove)
        examples = []
        collection = grove[config['mongo_db']][str(config['_id'])]

        focus_ids = collection.aggregate(
            [
                {
                    "$group": {
                        "_id": "$_geo.orig",
                        "lid": {
                            "$first": "$_id"
                        }
                    }
                },
                {"$limit": 5}
            ]
        )

        for doc in collection.find(
                {"_id": {"$in": [doc['lid'] for doc in focus_ids]}}):
            type_, query = nominatim.remap_documents(doc, mapping)
            if not query:
                continue
            examples.append({"i": query, "t": type_, "r": {"_id": doc["_id"]}})
            for attempt in range(5):
                geocode = nominatim.get_geocode_result(type_, query)
                query = getattr(geocode, "get_%s" % type_)(
                    query=query, attempt=attempt)
                if query:
                    examples[-1][str(attempt)] = getattr(
                        geocode, "res_%s" % type_)(query, set())
                else:
                    examples[-1][str(attempt)] = None
                examples[-1]["r"][str(attempt)] = geocode.result

        return examples

    def rerun_job(nominatim, config, grove, verbose=True):
        nominatim.process(config=config, verbose=verbose, subdomain=Ellipsis)

    from sys import argv
    _script, subdomain, job_info, run_type = argv[:4]
    subdomain = subdomain.lower()
    job_info = job_info.lower()
    run_type = run_type.lower()

    from bson import ObjectId
    from pprint import pprint
    from ta_common.mango import Grove
    from ta_common.mango.relational_object.jobs import Jobs

    grove = Grove.get_mango_helper(compute=True, distribution=False)
    if ObjectId.is_valid(subdomain):
        subdomain = str(subdomain)
    else:
        subdomain = str(grove[MC.APP_DB][MC.SUB_COL].find_one(
            {"sub": subdomain}, {"_id": 1})["_id"])

    job = Jobs.fetch(grove[subdomain], fid=job_info, current=True)
    job = job or Jobs.fetch(grove[subdomain], _id=job_info, current=True)

    if 'p' in run_type:  # print JSON-encoded job config
        print job.to_json(for_aws=True)

    if 'm' in run_type or 'e' in run_type or 'r' in run_type:
        nom = Nominatim.spawn_for_local(
            client=grove, nominatim_host=NOMINATIM_HOST)
        if 'm' in run_type:  # print job conf's mapping
            print "Mapping:"
            pprint(get_mapping(nom, job, grove))
            print
        if 'e' in run_type:  # print example from job
            print "Examples:"
            pprint(get_example(nom, job, grove))
            print

        if 'c' in run_type and 'r' in run_type:
            from cProfile import run
            print "Runtime Profile:"
            run('rerun_job(nom, job, grove, verbose=False)', sort='tottime')
            print
        elif 'r' in run_type:  # re-run geo for job
            print "Rerunning:"
            rerun_job(nom, job, grove)
        print
