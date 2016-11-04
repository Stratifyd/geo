from collections import Counter
from geoip2.database import Reader
from geoip2.models import City
from json import loads
from math import log10
from phonenumbers import format_number, PhoneNumber, PhoneNumberMatcher
from regex import compile as regex_compile, UNICODE as regex_U
from sys import stdout
from ta_common.geo.centroid import phones
from ta_common.geo.mapping import phone_codes
from time import sleep
from traceback import format_exc
from urllib import urlencode

__all__ = ('NominatimMixin', 'MaxmindMixin', 'PhoneNumberMixin')


class NominatimMixin(object):
    # String geocode and reverse geocode operations provided by Nominatim
    TYPICAL_GEOCODE_QUERY = "geocode"
    TYPICAL_GEOCODE_SCRIPT = "search.php?"
    ATTEMPT_GEOCODE_ADJUST = regex_compile(r'[^\p{L}\p{N}\p{M},]', regex_U)
    CONSIDERATION_PRIORITY = ('country', 'state', 'county',
                              'city', 'street', 'postalcode')

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
                self._calls -= 1
                wait = self._sleep.value * 10
                if self._debug:
                    stdout.write(
                        "\nDetected PostgreSQL database "
                        "error. Sleeping for %.2f seconds."
                        "\nBad Request: %s\n" % (wait, query))
                if "DB Error" in errors:
                    self._calls += 1
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
            print format_exc()
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
