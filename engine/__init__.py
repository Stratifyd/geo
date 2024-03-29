from collections import Container, Counter, Mapping, OrderedDict
from itertools import imap
from json import loads
from marshal import loads as load_obj, dumps as dump_obj
from math import log10
from multiprocessing import Process, Value
from multiprocessing.pool import ThreadPool
from os import walk
from os.path import join
from pymongo import MongoClient, DESCENDING
from pymongo.cursor import CursorType
from regex import compile as regex_compile, UNICODE as regex_U
from requests import Session, get as r_get
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, Timeout
from sys import stdout, maxunicode
from ta_common.field_names import DF, RO, MC, ENV
from geo.data.centroid import countries, regions, phones
from geo.data.mapping import (
    countries as country_names, regions as region_names, phone_codes)
from ta_common.text_tools.tokenizer import LanguageTokenizer
from threading import current_thread
from time import ctime, time, sleep
from traceback import format_exc
from unicodedata import normalize
from urllib import urlencode
from urlparse import urlparse, parse_qsl
from geo.engine.injection import (
    NominatimMixin, MaxmindMixin, PhoneNumberMixin)
from geo.helpers import CacheDictionary, LockedIterator
from geo.mapping import CentroidUpdateHelper
from warnings import warn


class Stroke(NominatimMixin, MaxmindMixin, PhoneNumberMixin):
    COUNTRY_KEYS = ("country", "continent")
    REGION_KEYS = ("state", "city", "town", "county",
                   "state_district", "village", "suburb")
    MAXIMUM_ATTEMPTS = 5

    # The following rely on Nominatim queries
    STANDARD_ARGUMENTS = dump_obj({
        "polygon_geojson": "0",
        "addressdetails": "1",
        # "countrycodes": "cn",  # Used for one-off jobs
        # "country": "China",  # Used for one-off jobs
        "format": "json"
    })
    STANDARD_KWARGUMENTS = {
        "limit": lambda limit: "%d" % min(max(int(limit), 1), 15),
        "countrycodes": lambda codes: "%s" % ','.join(codes)
    }

    @staticmethod
    def _fetch(url, **kwargs):
        return r_get(url, **kwargs).content

    __slots__ = ('__cached', '__calls', '__key', '__result', '_assoc',
                 '_cache', '_debug', '_maximum', '_minimum', '_ns', '_print',
                 '_query', '_sleep', '_type', 'arguments', 'exc', 'fxn',
                 'get_query', 'res_query')

    def __init__(self, host, query, assoc, search_type, _fetch_function=_fetch,
                 _catch_exceptions=(), _sleep=0.0, **kwargs):
        self._ns = host
        self.__key = None
        self._type = search_type
        self._assoc = assoc
        self._cache = kwargs.pop('cache', None)
        self.__calls = 0
        self._debug = kwargs.pop('debug', False)
        self._print = kwargs.pop('verbose', False)
        self._sleep = (_sleep if hasattr(_sleep, 'value')
                       else Value('f', _sleep))
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
        self._query = query
        self.get_query = getattr(self, 'get_%s' % self._type)
        self.res_query = getattr(self, 'res_%s' % self._type)
        self.arguments = load_obj(self.STANDARD_ARGUMENTS)

        self.__cached = False
        self.__result = None

        self._minimum = kwargs.pop('minimum_sleep', 0.0)
        self._maximum = kwargs.pop('maximum_sleep', 3.0)

        for kwarg in kwargs:
            call = self.STANDARD_KWARGUMENTS.get(kwarg, None)
            if isinstance(call, basestring):
                self.arguments += call % kwarg
            elif hasattr(call, '__call__'):
                self.arguments += call(kwarg)

    @property
    def sleep(self):
        return self._sleep.value

    @property
    def calls(self):
        if self.__result is None:
            self.result
        return self.__calls

    @calls.setter
    def calls(self, value):
        if isinstance(value, int) and -1 < value < self.MAXIMUM_ATTEMPTS:
            self.__calls = value

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
                try:
                    query = self.get_query(self._query, self.__calls)
                    if query is None:
                        self.__result = []
                        break

                    if self._cache:
                        if isinstance(query, dict):
                            key = CacheDictionary.gen_cache_key(query)
                        else:
                            key = CacheDictionary.gen_cache_key(
                                {self._type: query})
                        if key in self._cache:
                            if self._debug:
                                stdout.write("\nQuery #%d: '%s' (Cached)\n"
                                             % (self.__calls + 1, query))
                            self.__cached = True
                            self.__result = self._cache[key]
                            break

                    if self._debug:
                        stdout.write("\nQuery #%d: '%s'\n"
                                     % (self.__calls + 1, query))

                    try:
                        self.__calls += 1
                        docs = self.res_query(query, errors)
                        with self._sleep.get_lock():
                            self._sleep.value = max(
                                self._minimum,
                                self._sleep.value * 0.99)
                    except self.exc as err:
                        self.__calls -= 1
                        docs = None
                        if type(err) in errors:
                            self.__calls += 2
                            errors.remove(type(err))
                        else:
                            errors.add(type(err))
                        if self._print:
                            stdout.write(
                                "\nEncountered expected %s error on %s.\n%s"
                                % (self._type, query, format_exc()))
                        with self._sleep.get_lock():
                            self._sleep.value = min(
                                self._maximum,
                                self._sleep.value + 0.05)
                        sleep(self._sleep.value)
                    except:
                        with self._sleep.get_lock():
                            self._sleep.value = min(
                                self._maximum,
                                max(self._sleep.value, 0.05) * 2.5)
                        if self._print:
                            stdout.write(
                                "\nEncountered unknown %s error on %s!\n%s"
                                % (self._type, query, format_exc()))
                        docs = None
                        sleep(self._sleep.value)
                    if docs:
                        if not isinstance(docs, list):
                            docs = [docs]
                        if self._cache and key:
                            self.__key = key
                        self.__result = docs
                        break
                    else:
                        if self._cache:
                            mirror.add(key)
                        sleep(self._sleep.value)
                except:
                    self.__calls += 1
                    if self._print:
                        stdout.write("\nEncountered unknown global error!\n%s"
                                     % format_exc())
                    sleep(self._sleep.value)

            try:
                if not self.__cached:
                    self.__result = map(self.__normalize, self.__result)
                    if self._cache:
                        self._cache[key] = self.__result
                if self._cache and key:
                    for alt in mirror:
                        self._cache.register_alternate(key, alt)
            except:
                if self._print:
                    stdout.write("\nEncountered error with normalization!\n%s"
                                 % format_exc())
                self.__result = []

            if self._debug and (self.__calls + 1) == self.MAXIMUM_ATTEMPTS:
                stdout.write("\nQuery exhausted all attempts [%s]: %s\n"
                             % (current_thread().ident, self._query))
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

        result = {}

        try:
            result["id"] = int(dictionary["osm_id"])
        except:
            result["id"] = -1

        try:
            result["full"] = unicode(dictionary["display_name"])
        except:
            result["full"] = u""

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
            if self._debug:
                stdout.write("\nEncountered issue with result normalization!"
                             "\n%s" % format_exc())
            latitude = None
            longitude = None

        try:
            country_code, region_code = self._assoc.codify(
                latitude=latitude, longitude=longitude,
                country_strings=country_args, region_strings=region_args)

            country_code = country_code.upper() if country_code else None
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
                        "country_name": country_names.get(country_code, None),

                        "region": region_code if region_code else None,
                        "region_name": region_names.get(country_code, {}).get(
                            region_code, None),

                        "postal": address.get("postcode", None)
                    }
                }
            )
        except:
            if self._debug:
                stdout.write("\nEncountered issue with result normalization!"
                             "\n%s" % format_exc())
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


class Piston(object):
    OPTION_PRIORITY_ORDER = (
        ("phone_number", PhoneNumberMixin.PHONE_NUMBER_QUERY),
        ("ip_address", MaxmindMixin.IP_ADDRESS_QUERY),
        ("latitude_longitude", NominatimMixin.REVERSE_GEOCODE_QUERY),
        ("longitude_latitude", NominatimMixin.REVERSE_GEOCODE_QUERY),
        ("latitude", NominatimMixin.REVERSE_GEOCODE_QUERY),
        ("global", NominatimMixin.TYPICAL_GEOCODE_QUERY),
        ("subglobal", NominatimMixin.TYPICAL_GEOCODE_QUERY),
        ("local", NominatimMixin.TYPICAL_GEOCODE_QUERY),
        ("sublocal", NominatimMixin.TYPICAL_GEOCODE_QUERY),
        ("postcode", NominatimMixin.TYPICAL_GEOCODE_QUERY),
    )

    LEGAL_CONFIGURATION_OPTIONS = tuple(
        option for option, _ in OPTION_PRIORITY_ORDER
    ) + ("longitude", "unknown")

    @property
    def state(self):
        return (self.HITS.value, self.MISS.value, self.CONT.value,
                self.CODE.value, self.FUZZ.value, self.NULL.value,
                self.FAIL.value)

    @property
    def processed(self):
        return self.__processed.value

    @classmethod
    def spark(cls, directory='/', client=Ellipsis, configuration=None,
              nominatim_host=None, **kwargs):
        country_geocode = None
        region_geocode = None
        phone_geocode = None

        if configuration is None:
            if nominatim_host is None:
                raise ValueError("Cannot run without a configuration and a "
                                 "known Nominatim host address!")
            for dirname, _dirpath, filenames in walk(directory):
                if country_geocode is None and 'cgeo.json.xz' in filenames:
                    country_geocode = join(dirname, 'cgeo.json.xz')
                if region_geocode is None and 'rgeo.json.xz' in filenames:
                    region_geocode = join(dirname, 'rgeo.json.xz')
                if phone_geocode is None and 'pgeo.json.xz' in filenames:
                    phone_geocode = join(dirname, 'pgeo.json.xz')
                if country_geocode and region_geocode and phone_geocode:
                    break
        else:
            nominatim_host = nominatim_host or configuration.getNominatimURI()
            country_geocode = configuration.getNominatimCountryGeoJSON()
            region_geocode = configuration.getNominatimRegionGeoJSON()
            phone_geocode = configuration.getNominatimPhoneGeoJSON()

        nominatim_host = urlparse(nominatim_host)
        nominatim_host = "%s://%s/nominatim/" % (
            nominatim_host.scheme or 'http',
            nominatim_host.netloc or nominatim_host.path)

        if country_geocode and region_geocode:
            configuration = {'_country_geocode': country_geocode,
                             '_region_geocode': region_geocode,
                             'verbose': ENV.get(ENV.VERBOSE, as_type=int) > 2}
            if phone_geocode:
                configuration['_phone_geocode'] = phone_geocode
            kwargs.update(configuration)
            return cls(client, nominatim_host, **kwargs)
        raise ValueError("Cannot initialize geo.engine.Piston without "
                         "country and region geocode mappings.")

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

    }
    HC.update({  # Chinese main provinces
        province: province + u'\u7701'
        for province in {
            u'\u6cb3\u5317',
            u'\u5c71\u897f',
            u'\u8fbd\u5b81',
            u'\u5409\u6797',
            u'\u9ed1\u9f99\u6c5f',
            u'\u6c5f\u82cf',
            u'\u6d59\u6c5f',
            u'\u5b89\u5fbd',
            u'\u798f\u5efa',
            u'\u6c5f\u897f',
            u'\u6cb3\u5357',
            u'\u5c71\u4e1c',
            u'\u6e56\u5317',
            u'\u6e56\u5357',
            u'\u5e7f\u4e1c',
            u'\u6d77\u5357',
            u'\u56db\u5ddd',
            u'\u8d35\u5dde',
            u'\u4e91\u5357',
            u'\u7518\u8083',
            u'\u9752\u6d77',
            u'\u53f0\u6e7e',
        }})
    HC[u'\u9655\u897f'] = HC[u'\u9655\u897f\u7701'] = "Shaanxi"  # HATE
    HC[u'\u5c71\u897f'] = HC[u'\u5c71\u897f\u7701'] = "Shanxi"  # HAATE
    HC.update({  # Chinese major cities
        city: city + u'\u5e02'
        for city in {
            u'\u5317\u4eac',
            u'\u5929\u6d25',
            u'\u4e0a\u6d77',
            u'\u91cd\u5e86',
        }})
    HC.update({  # Chinese autonomous regions
        auto: auto + u'\u81ea\u6cbb\u533a'
        for auto in {
            u'\u5167\u8499\u53e4',
            u'\u5e7f\u897f\u58ee',
            u'\u897f\u85cf',
            u'\u5b81\u590f\u56de\u65cf',
            u'\u65b0\u7586\u7ef4\u543e\u5c14',
        }})
    HC.update({  # Chinese "Special" administrative regions
        spec: spec + u'\u7279\u522b\u884c\u653f\u533a'
        for spec in {
            # u'\u9999\u6e2f', # This is Hong Kong
            u'\u6fb3\u95e8',
        }})

    def remap_documents(self, document, mapping):
        information = {field_type: [] for field_type in mapping.values()}
        for field_name, field_type in mapping.iteritems():
            information[field_type].append(document.get(field_name))
        return self.remap_information(information)

    def remap_information(self, information):
        for field_type in information.iterkeys():
            information[field_type] = normalize('NFKC', u' '.join(
                filter(None, (p.strip() if isinstance(p, basestring) else u''
                              for p in information[field_type]))))
            if self.NS.intersection(information[field_type]):
                information[field_type] = self.tokenizer(
                    'zh', information[field_type])
            information[field_type] = self.HC.get(
                information[field_type], information[field_type])

        for field_type, search_type in self.OPTION_PRIORITY_ORDER:
            geo_lookup = getattr(self, '_' + field_type)(information)
            if isinstance(geo_lookup, dict):
                return search_type, geo_lookup

        return NominatimMixin.TYPICAL_GEOCODE_QUERY, self._unknown(information)

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

    CN = regex_compile(r'([\p{L}\p{N}]\P{Z}*[\p{L}\p{N}],?)', regex_U)

    def _global(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            global_ = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('global', u''))).strip()
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
                    information.get('subglobal', u''))).strip()
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
                    information.get('local', u''))).strip()
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
                    information.get('sublocal', u''))).strip()
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
                    information.get('postcode', u''))).strip()
            if postcode:
                carry['postalcode'] = postcode
            return self._unknown(information, carry, extra)
        except:
            return None

    def _unknown(self, information, carry=None, extra=None):
        try:
            carry, extra = self.__clean_carry_and_extra(carry, extra)
            unknown = u' '.join(
                match.group() for match in self.CN.finditer(
                    information.get('unknown', u''))).strip()
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

    @staticmethod
    def spawn_session(namespace, concurrency=4):
        session = Session()
        session.mount(prefix=namespace,
                      adapter=HTTPAdapter(pool_connections=concurrency,
                                          pool_maxsize=concurrency * 2,
                                          max_retries=0,
                                          pool_block=False))
        return session

    def __init__(self, client, nominatim_host, **kwargs):
        if isinstance(client, MongoClient):
            self.__client = client
        elif client is Ellipsis:
            warn("Ignoring invalid client -- cannot run jobs!", RuntimeWarning)
        else:
            raise TypeError("Nominatim must be started with a MongoClient!")

        self.__ns = kwargs['nominatim_host'] = nominatim_host
        warn("Geocoding against %s." % self.__ns, UserWarning)
        self.__map = CentroidUpdateHelper(**kwargs)
        self.__used = set()
        self.__cache = CacheDictionary(maxsize=kwargs.get('maxsize', 100000),
                                       weakref=False)
        self.__thread = kwargs.get('concurrent', 4)
        self.__session = self.spawn_session(namespace=self.__ns,
                                            concurrency=self.__thread)
        self.__processed = Value('i', 0, lock=False)
        self.__sleep = Value('f', 0.0, lock=True)
        self.tokenizer = LanguageTokenizer(concurrent=True)
        self.concurrent = kwargs.get('concurrent', 4)

    def session_fetch_function(self, url, **kwargs):
        return self.__session.get(url, timeout=5.0, **kwargs).content

    def restore_from_cache(self, subdomain, limit=None):
        if isinstance(limit, int):
            limit = int(max(min(limit, self.__cache.maxsize),
                            CacheDictionary.CACHE_SIZE_MIN // 100))
        else:
            limit = self.__cache.maxsize

        self.__cache = CacheDictionary(maxsize=self.__cache.maxsize,
                                       weakref=False)
        for doc in self.__client[subdomain][MC.CACHE_COL].find().sort(
                RO.LAST, DESCENDING).limit(limit):
            if 'value' in doc and doc['value']:
                search_query = doc.pop(RO.OBJECT_ID)
                self.__cache[CacheDictionary.gen_cache_key(search_query)
                             ] = doc['value']

    def update_mongo_cache(self, subdomain):
        action = time() * 1000.0
        bulk = self.__client[
            subdomain][MC.CACHE_COL].initialize_unordered_bulk_op()
        for key in self.__used:
            if not key:
                continue
            _id = CacheDictionary.restore_cache_key(key)
            val = self.__cache.quiet_get(key)
            if val:
                bulk.find({RO.OBJECT_ID: _id}).upsert().update(
                    {'$set': {'value': val, RO.LAST: action}})
        bulk.execute()

    def process(self, config, subdomain=None, pool_size=4, verbose=False):
        for _ in self.iterprocess(config, subdomain, pool_size, verbose):
            pass

    def _report_status_oneline(self, locked, runtime):
        stdout.write(
            "[% 9.3f] %d hits / %d misses / %d calls <--> "
            "%d coded (nom) / %d coded (idf) / %d empty <--> "
            "%04.2f sleep / %03.2f codes / %d total ~~ %d in iterlock.\r" % (
                time() - runtime,

                self.HITS.value,
                self.MISS.value,
                self.CONT.value,

                self.CODE.value,
                self.FUZZ.value,
                self.NULL.value,

                self.__sleep.value,
                float(self.HITS.value + self.MISS.value) / (time() - runtime),
                self.HITS.value + self.MISS.value + self.FAIL.value,

                locked))
        stdout.flush()

    def _report_status_compact(self, locked, runtime):
        stdout.write(
            "[% 9.3f] %d in iterlock (%d processed, %.3f per second)\n"
            "     Cache: %d hits / %d misses\n"
            "    Result: %d results -> %d codified\n"
            "   Network: %d calls (sleeping for %04.2f seconds)\n"
            " Timestamp: %s\n" % (
                time() - runtime,
                locked,
                self.HITS.value + self.MISS.value + self.FAIL.value,
                float(self.HITS.value + self.MISS.value) / (time() - runtime),

                self.HITS.value,
                self.MISS.value,

                self.CODE.value,
                self.FUZZ.value,

                self.CONT.value,
                self.__sleep.value,

                ctime(),
            ))
        stdout.flush()

    def report_status(self, locked, runtime):
        return self._report_status_compact(locked, runtime)

    def iterprocess(self, config, subdomain=None, pool_size=4, verbose=False):
        if verbose:
            runtime = time()
        self.__processed = Value('i', 0, lock=True)
        self.HITS = Value('i', 0, lock=True)
        self.MISS = Value('i', 0, lock=True)
        self.CONT = Value('i', 0, lock=True)
        self.CODE = Value('i', 0, lock=True)
        self.FUZZ = Value('i', 0, lock=True)
        self.NULL = Value('i', 0, lock=True)
        self.FAIL = Value('i', 0, lock=True)

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
        pool = ThreadPool(min(max(int(pool_size), 1), self.concurrent))

        bulk = self.__client[config.mongo_db][
            config.mongo_table].initialize_unordered_bulk_op()
        locked = LockedIterator(
            self.__client[config.mongo_db][config.mongo_table].find(
                {}, projection={field: 1 for field in self._config}),
            lock_past=self.concurrent * 2150)

        if verbose:
            self.report_status(len(locked), runtime)
            last = time()

        for _id, geo, err in pool.imap_unordered(self.__process, locked):
            # for _id, geo, err in imap(self.__process, locked):
            locked -= 1
            self.__processed.value += 1
            if verbose and (time() - last) > 5.0:
                self.report_status(len(locked), runtime)
                last = time()
            if not _id or err is not None:
                stdout.write('\n%s\n' % err)
                yield err
                continue
            bulk.find({RO.OBJECT_ID: _id}).update_one({'$set': {DF.geo: geo}})
            yield None

        if verbose:
            self.report_status(len(locked), runtime)
            stdout.write("\n[% 9.3f] Geocoding complete.\n"
                         % (time() - runtime))

        pool.close()
        pool.join()

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
        yield None
    
    def iterprocess_streaming(self, docs, model, subdomain=None, pool_size=4, verbose=False):
        if verbose:
            runtime = time()
        self.__processed = Value('i', 0, lock=True)
        self.HITS = Value('i', 0, lock=True)
        self.MISS = Value('i', 0, lock=True)
        self.CONT = Value('i', 0, lock=True)
        self.CODE = Value('i', 0, lock=True)
        self.FUZZ = Value('i', 0, lock=True)
        self.NULL = Value('i', 0, lock=True)
        self.FAIL = Value('i', 0, lock=True)

        
        if subdomain:
            if verbose:
                stdout.write("[% 9.3f] Beginning retrieval of mongo cache...\n"
                             % (time() - runtime))
            self.restore_from_cache(subdomain=subdomain,
                                    limit=10000)
            if verbose:
                stdout.write("[% 9.3f] Retrieval of mongo cache complete.\n"
                             % (time() - runtime))
        
        self._config = self.generate_field_mapping(model)
        pool = ThreadPool(min(max(int(pool_size), 1), self.concurrent))

        #bulk = self.__client[config.mongo_db][
        #    config.mongo_table].initialize_unordered_bulk_op()
            
            
        locked = LockedIterator(docs, lock_past=self.concurrent * 2150)

        if verbose:
            self.report_status(len(locked), runtime)
            last = time()

        for _id, geo, err in pool.imap_unordered(self.__process, locked):
            # for _id, geo, err in imap(self.__process, locked):
            locked -= 1
            self.__processed.value += 1
            if verbose and (time() - last) > 5.0:
                self.report_status(len(locked), runtime)
                last = time()
            if not _id or err is not None:
                stdout.write('\n%s\n' % err)
                #yield err
                continue
            yield (_id, geo)

        if verbose:
            self.report_status(len(locked), runtime)
            stdout.write("\n[% 9.3f] Geocoding complete.\n"
                         % (time() - runtime))

        pool.close()
        pool.join()

        if verbose:
            stdout.flush()
            stdout.write("[% 9.3f] Subthreads joined.\n"
                         % (time() - runtime))

    
    
    def fire(self, query, type_=None):
        if type_ is None:
            type_, query = self.remap_information(query)
        return Stroke(host=self.__ns,
                      query=query,
                      assoc=self.__map,
                      cache=self.__cache,
                      search_type=type_,
                      verbose=True,
                      debug=False,
                      _fetch_function=self.session_fetch_function,
                      _catch_exceptions=(ConnectionError, Timeout),
                      _sleep=self.__sleep)

    def __process(self, dictionary):
        try:
            _id = dictionary[RO.OBJECT_ID]

            type_, query = self.remap_documents(dictionary, self._config)
            if isinstance(query, dict):
                geocode = self.fire(query, type_)

                if geocode.call_was_cached:
                    with self.HITS.get_lock():
                        self.HITS.value += 1
                    self.__used.add(geocode.cache_key)
                else:
                    with self.MISS.get_lock():
                        self.MISS.value += 1
                with self.CONT.get_lock():
                    self.CONT.value += geocode.calls

                if geocode.result:
                    result = geocode.result[0]
                else:
                    result = {}

                if result.get('id') > 0 or result.get('full'):
                    with self.CODE.get_lock():
                        self.CODE.value += 1

                if result.get('code', {}).get('country'):
                    with self.FUZZ.get_lock():
                        self.FUZZ.value += 1
                else:
                    with self.NULL.get_lock():
                        self.NULL.value += 1

                if query.get('orig'):
                    result['orig'] = query['orig']
            else:
                with self.FAIL.get_lock():
                    self.FAIL.value += 1
                result = {}

            return _id, result, None
        except:
            with self.FAIL.get_lock():
                self.FAIL.value += 1
            return None, {}, format_exc()


class Engine(object):

    @property
    def processed(self):
        return sum(piston.processed for piston in self)

    def __init__(self, grove, num_processes=1):
        pass

    @classmethod
    def turn_over(cls, grove=None, configuration=None):
        pass
