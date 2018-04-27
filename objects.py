from json import dumps
from traceback import format_exc
from warnings import warn

try:
    from osgeo.ogr import (CreateGeometryFromJson, Geometry,
                           wkbPoint, wkbLinearRing, wkbPolygon)

    def Point(longitude, latitude, altitude=0.0):
        point = Geometry(wkbPoint)
        point.AddPoint(longitude, latitude, altitude)
        return point

    def create_from_coordinates(longitude_latitude_coordinates):
        ring = Geometry(wkbLinearRing)
        for longitude, latitude in longitude_latitude_coordinates:
            ring.AddPoint(longitude, latitude)
        ring.CloseRings()
        poly = Geometry(wkbPolygon)
        poly.AddGeometry(ring)
        return poly

    def centroid(geo):
        return geo.PointOnSurface().GetPoint()[:2][::-1]

    def contains(geo, other):
        return not geo.Disjoint(other)

    def create_from_geojson(dictionary):
        return CreateGeometryFromJson(dumps(dictionary))

    def distance(geo, other):
        return geo.Distance(other)

    def iter_points(geo):
        for point in geo.Boundary().GetPoints():
            yield point

    def transform_to_geojson(geo):
        return geo.ExportToJson()

    def union(geo, other):
        return geo.Union(other)

    warn("Geocode objects are backed by the C extension GDAL.", UserWarning)
except:
    gdal_error = format_exc()
    try:
        from shapely.geometry import shape, mapping, Point, Polygon

        def create_from_coordinates(longitude_latitude_coordinates):
            return Polygon(longitude_latitude_coordinates)

        def centroid(geo):
            return shape(geo).representative_point().coords[0][::-1]

        def contains(geo, other):
            return geo.contains(other)

        def create_from_geojson(dictionary):
            return shape(dictionary)

        def distance(geo, other):
            return geo.distance(other)

        def iter_points(geo):
            for c0, c1 in geo.exterior.coords:
                yield (c0, c1, 0.0)

        def transform_to_geojson(geo):
            return mapping(geo)

        def union(geo, other):
            return geo.union(other)

        warn("Geocode objects are backed by the Python extension Shapely.",
             UserWarning)
    except:
        raise ImportError(
            "GeoMapping object failed to instantiate;\nGDAL: %s\n\nShapely: %s"
            % (gdal_error, format_exc()))
