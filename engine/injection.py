from collections import Counter
from geoip2.database import Reader
from geoip2.models import City
from json import loads
from math import log10
from phonenumbers import format_number, PhoneNumberMatcher
from regex import compile as regex_compile, UNICODE as regex_U
from sys import stdout, maxunicode
from geo.data.centroid import phones
from geo.data.mapping import phone_codes
from time import sleep
from traceback import format_exc
from urllib import urlencode

__all__ = ('NominatimMixin', 'MaxmindMixin',
           'PhoneNumberMixin', 'PostalCodeMixin')


class NominatimResponseError(StandardError):

    def __init__(self, querystring, responsebody, message=""):
        self.q = querystring
        self.b = responsebody
        if isinstance(message, unicode):
            self.m = message.encode('utf-8', 'ignore')
        else:
            self.m = str(message)

    def __repr__(self):
        return ("NominatimResponseError:" +
                "\n  Submitted Query: " + self.q +
                "\n  Response Body:\n " + self.b.replace("\n", "\n    ") +
                "\n" + self.m)

    def __str__(self):
        return repr(self)


class NominatimMixin(object):
    NS = regex_compile(r'[\p{script=Han}\p{script=Tibetan}\p{script=Lao}'
                       r'\p{script=Thai}\p{script=Khmer}]', regex_U)
    NS = frozenset(NS.findall(u''.join(unichr(i) for i in xrange(maxunicode))))
    # String geocode and reverse geocode operations provided by Nominatim
    TYPICAL_GEOCODE_QUERY = "geocode"
    TYPICAL_GEOCODE_SCRIPT = "search.php?"
    ATTEMPT_GEOCODE_ADJUST = regex_compile(r'[^\p{L}\p{N}\p{M},]', regex_U)
    CONSIDERATION_PRIORITY = (
        'street', 'postalcode', 'county', 'city', 'state', 'country')
    CONSIDERATION_ATTEMPTS = tuple(
        map(frozenset, (
            ('country', 'state', 'city', 'street'),
            ('country', 'state', 'city', 'county'),
            ('country', 'postalcode'),
            ('country', 'state', 'city',),
            ('state', 'city'),
            ('country', 'state'),
            ('country', 'city'),
            ('state', 'county'),
            ('country', 'street'),
            ('state', 'street'),
            ('city', 'street'),
            ('county', 'street'),
            ('postalcode', 'street'),
            ('country',),
            ('state',),
            ('postalcode',),
            ('city',),
            ('street',),
        )))
    BLACKLIST_PHRASES = frozenset([
        "other", "n/a", "none", "unknown", "nowhere", "null",
        u'\u6d77\u5916', u'\u5176\u4ed6', u'\u5176\u5b83'])
    __slots__ = ()  # This class simply stores methods, no __dict__ needed.

    @staticmethod
    def __urlencode_query(params):
        return urlencode(sorted([(k.encode('utf-8'), v.encode('utf-8'))
                                 for k, v in params.items()]))

    def run_geocode(self, query, errors):
        body = self.fxn(self._ns + query)
        try:
            return loads(body.strip())
        except:
            if "DB Error" in body:
                self.calls -= 1
                wait = self._sleep.value * 10
                if self._debug:
                    stdout.write(
                        "\nDetected PostgreSQL database "
                        "error. Sleeping for %.2f seconds."
                        "\nBad Request: %s\n" % (wait, query))
                if "DB Error" in errors:
                    self.calls += 1
                    errors.remove("DB Error")
                else:
                    errors.add("DB Error")
                sleep(wait)
            elif "Internal Server Error" in body:
                if self._debug:
                    stdout.write(
                        "\nDetected Nominatim internal error."
                        "\nBad Request: %s\n" % query)
                sleep(self._sleep.value)
            else:
                if self._debug:
                    stdout.write(
                        "\nEncountered unknown error.\n%s\n%s"
                        "\nBad Request: %s\n"
                        % (body, format_exc(), query))
                sleep(self._sleep.value)
            raise NominatimResponseError(
                query, body, "Response was not legal JSON.")

    @classmethod
    def get_geocode(cls, query, attempt=0, juggle=False):
        for value in query.itervalues():
            substrings = list(filter(None, value.lower().strip().split()))
            if len(substrings) > 2:
                continue
            for substring in substrings:
                for ignore in cls.BLACKLIST_PHRASES:
                    if substring.startswith(ignore):
                        return None

        params = {field: query[field] for field in cls.CONSIDERATION_PRIORITY
                  if field in query and query[field]}
        if params:
            for keyset in cls.CONSIDERATION_ATTEMPTS:
                if keyset.difference(params):
                    continue
                if attempt > 0:
                    attempt -= 1
                else:
                    return {field: params[field] for field in keyset}

        if any(cls.NS.intersection(value) for value in query.itervalues()):
            left2right = 0
        else:
            left2right = 1

        if attempt > 0:
            if juggle:
                attempt += 1
                if attempt % 2 == left2right:
                    cut = slice(int(attempt // 2), None, None)
                else:
                    cut = slice(None, -int(attempt // 2), None)
            else:
                if left2right:
                    cut = slice(int(attempt), None, None)
                else:
                    cut = slice(None, -int(attempt), None)
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
        query.update(self.arguments)
        return self.run_geocode(self.TYPICAL_GEOCODE_SCRIPT +
                                self.__urlencode_query(query), errors)

    REVERSE_GEOCODE_QUERY = "reverse"
    REVERSE_GEOCODE_SCRIPT = "reverse.php?"

    @classmethod
    def get_reverse(cls, query, attempt=0):
        # For an explanation of recorded accuracy see:
        # https://en.wikipedia.org/wiki/Decimal_degrees#Precision
        if attempt == 0:
            return {"lat": query["lat"], "lon": query["lon"]}
        else:
            return None

    def res_reverse(self, query, errors):
        query.update(self.arguments)
        return self.run_geocode(self.REVERSE_GEOCODE_SCRIPT +
                                self.__urlencode_query(query), errors)


class MaxmindMixin(object):
    # IP address lookup provided by an internal MaxMind database
    IP_ADDRESS_QUERY = "ip_address"
    MAXMIND = None

    @classmethod
    def __spawn_maxmind(cls, configuration=None, **ignored):
        if cls.MAXMIND is None:
            if configuration is None:
                from ta_common.taste_config_helper import TasteConf
                configuration = TasteConf()
            cls.MAXMIND = Reader(configuration.getMaxMindSource())
        return cls.MAXMIND

    __slots__ = ()  # This class simply stores methods, no __dict__ needed.

    def get_ip_address(self, query, attempt=0):
        try:
            return query[self.IP_ADDRESS_QUERY][attempt]
        except:
            return None

    def res_ip_address(self, query, errors):
        try:
            result = self.__spawn_maxmind().city(query)
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


class PostalCodeMixin(object):
    # Postal code lookup currently augmented via ad-hoc lookup guesswork
    POSTAL_CODE_QUERY = "postalcode"
    __slots__ = ()  # This class simply stores methods, no __dict__ needed.

    def get_postalcode(self, query, attempt=0):
        pass

    def res_postalcode(self, query, errors):
        pass


class PhoneNumberMixin(object):
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

    __slots__ = ('_numbers', '_matched')

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
