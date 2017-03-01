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
from requests import session, get as r_get
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, Timeout
from sys import stdout, maxunicode
from ta_common.field_names import DF, RO, MC, ENV
from ta_common.geo.centroid import countries, regions, phones
from ta_common.geo.mapping import (
    countries as country_names, regions as region_names, phone_codes)
from ta_common.text_tools.tokenizer import LanguageTokenizer
from time import time, sleep
from traceback import format_exc
from unicodedata import normalize
from urllib import urlencode
from urlparse import urlparse, parse_qsl
from v2_tier3_compute_node.geo.engine.injection import (
    NominatimMixin, MaxmindMixin, PhoneNumberMixin)
from v2_tier3_compute_node.geo.helpers import LockedIterator, CacheDictionary
from v2_tier3_compute_node.geo.mapping import CentroidUpdateHelper
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

    __slots__ = ('_ns', '__key', '_type', '_assoc', '_cache', '__calls', 'exc',
                 '_debug', '_query', '__cached', '__result', '_minimum', 'fxn',
                 '_maximum', '_sleep', 'arguments', 'get_query', 'res_query')

    def __init__(self, host, query, assoc, search_type, _fetch_function=_fetch,
                 _catch_exceptions=(), _sleep=0.0, **kwargs):
        self._ns = host
        self.__key = None
        self._type = search_type
        self._assoc = assoc
        self._cache = kwargs.pop('cache', None)
        self.__calls = 0
        self._debug = kwargs.pop('debug', False)
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
                                stdout.write("Query #%d: '%s' (Cached)\n"
                                             % (self.__calls + 1, query))
                            self.__cached = True
                            self.__result = self._cache[key]
                            break

                    if self._debug:
                        stdout.write("Query #%d: '%s'\n"
                                     % (self.__calls + 1, query))

                    try:
                        self.__calls += 1
                        docs = self.res_query(query, errors)
                        with self._sleep.get_lock():
                            self._sleep.value = max(self._minimum,
                                                    self._sleep.value * 0.99)
                    except self.exc as err:
                        self.__calls -= 1
                        docs = None
                        if type(err) in errors:
                            self.__calls += 1
                            errors.remove(type(err))
                        else:
                            errors.add(type(err))
                        if self._debug:
                            stdout.write("\nEncountered expected %s error!\n%s"
                                         % (self._type, format_exc()))
                        sleep(self._sleep.value)
                    except:
                        with self._sleep.get_lock():
                            self._sleep.value = max(self._maximum,
                                                    self._sleep.value * 1.05)
                        if self._debug:
                            stdout.write("\nEncountered unknown %s error!\n%s"
                                         % (self._type, format_exc()))
                        docs = None
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
                except:
                    self.__calls += 1
                    if self._debug:
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
                if self._debug:
                    stdout.write("\nEncountered issue with normalization!\n%s"
                                 % format_exc())
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
    LANGUAGE_TOKENIZER = None
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

    @classmethod
    def _get_class_tokenizer(cls):
        if cls.LANGUAGE_TOKENIZER is None:
            cls.LANGUAGE_TOKENIZER = LanguageTokenizer()
        return cls.LANGUAGE_TOKENIZER

    @property
    def tokenizer(self):
        return self._get_class_tokenizer()

    @property
    def state(self):
        return (self.HITS.value, self.MISS.value, self.CONT.value,
                self.CODE.value, self.FUZZ.value, self.NULL.value)

    @property
    def processed(self):
        return self.processed.value

    @classmethod
    def spark(cls, directory='/', client=Ellipsis, configuration=None,
              nominatim_host='http://nominatim.openstreetmap.org/',
              **kwargs):
        country_geocode = None
        region_geocode = None
        phone_geocode = None

        if configuration is None:
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
            nominatim_host = configuration.getNominatimHost()
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
        return self.remap_information(information)

    def remap_information(self, information):
        for field_type in information.iterkeys():
            information[field_type] = normalize('NFKC', u' '.join(
                filter(None, (p.strip() for p in information[field_type]))))
            if self.NS.intersection(information[field_type]):
                information[field_type] = (
                    self.tokenizer.zh(information[field_type]))
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

    def __init__(self, client, nominatim_host, **kwargs):
        if isinstance(client, MongoClient):
            self.__client = client
        elif client is Ellipsis:
            warn("Ignoring invalid client -- cannot run jobs!", RuntimeWarning)
        else:
            raise TypeError("Nominatim must be started with a MongoClient!")

        self.__ns = kwargs['nominatim_host'] = nominatim_host
        self.__map = CentroidUpdateHelper(**kwargs)
        self.__used = set()
        self.__cache = CacheDictionary(maxsize=kwargs.get('maxsize', 100000),
                                       weakref=False)
        self.__session = session()
        self.__session.mount(
            prefix=self.__ns,
            adapter=HTTPAdapter(pool_connections=kwargs.get('concurrent', 4),
                                pool_maxsize=kwargs.get('concurrent', 4) * 2,
                                pool_block=True))
        self.__processed = Value('i', 0, lock=False)
        self.__sleep = Value('f', 0.0, lock=True)
        self.concurrent = kwargs.get('concurrent', 4)

    def session_fetch_function(self, url, **kwargs):
        return self.__session.get(url, timeout=30.0, **kwargs).content

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

    def report_status(self, locked, runtime):
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
                self.HITS.value + self.MISS.value,

                locked))
        stdout.flush()

    def iterprocess(self, config, subdomain=None, pool_size=4, verbose=False):
        if verbose:
            runtime = time()
        self.__processed = Value('i', 0, lock=False)
        self.HITS = Value('i', 0, lock=False)
        self.MISS = Value('i', 0, lock=False)
        self.CONT = Value('i', 0, lock=False)
        self.CODE = Value('i', 0, lock=False)
        self.FUZZ = Value('i', 0, lock=False)
        self.NULL = Value('i', 0, lock=False)

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
        pool = ThreadPool(min(max(int(pool_size), 0), self.concurrent))

        bulk = self.__client[config.mongo_db][
            config.mongo_table].initialize_unordered_bulk_op()
        locked = LockedIterator(
            self.__client[config.mongo_db][config.mongo_table].find(
                {}, projection={field: 1 for field in self._config}),
            lock_past=self.concurrent * 2150)

        if verbose:
            self.report_status(len(locked), runtime)
            last = time()

        for _id, geo in pool.imap_unordered(self.__process, locked):
            # for _id, geo in imap(self.__process, locked):
            locked -= 1
            if verbose and (time() - last) > 0.5:
                self.report_status(len(locked), runtime)
                last = time()
            if not _id:
                continue
            bulk.find({RO.OBJECT_ID: _id}).update_one({'$set': {DF.geo: geo}})
            self.__processed.value += 1
            yield geo

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

    def fire(self, query, type_=None):
        if type_ is None:
            type_, query = self.remap_information(query)
        return Stroke(host=self.__ns,
                      query=query,
                      assoc=self.__map,
                      cache=self.__cache,
                      search_type=type_,
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
                    self.HITS.value += 1
                    self.__used.add(geocode.cache_key)
                else:
                    self.MISS.value += 1
                self.CONT.value += geocode.calls

                if geocode.result:
                    result = geocode.result[0]
                else:
                    result = {}

                if result.get('id') > 0 or result.get('full'):
                    self.CODE.value += 1

                if result.get('code', {}).get('country'):
                    self.FUZZ.value += 1
                else:
                    self.NULL.value += 1

                result['orig'] = query.get('orig', '')
            else:
                result = {}

            return _id, result
        except:
            return None, {}


class Engine(object):

    def report_status(self, overall_runtime):
        hits, miss, cont, code, fuzz, null = (
            state / len(self) for state in map(
                sum, zip(piston.state for piston in self)))
        stdout.write("[% 9.3f] %d hits / %d misses / %d calls <--> "
                     "%d coded (nom) / %d coded (idf) / %d empty <--> "
                     "%04.2f sleep / %03.2f codes / %d total\r" % (
                         time() - overall_runtime,

                         hits,
                         miss,
                         miss + cont,

                         code,
                         fuzz,
                         null,

                         self.__sleep.value,
                         float(hits + miss) / (time() - overall_runtime),
                         hits + miss))
        stdout.flush()

    @property
    def processed(self):
        return sum(piston.processed for piston in self)

    def __init__(self, grove, num_processes=1):
        pass

    @classmethod
    def turn_over(cls, grove=None, configuration=None):
        pass
