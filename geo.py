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
  @author: Jeremy Villalobos
  @project: Unstructure Text Visualization
  @created: Jan 19, 2016
"""

from bson.son import SON
from geo.data.centroid import countries as kGeoCentroid
from geo.data.mapping import (region_names as ReverseRegion,
                              countries as CountryReverseMap)
from v2_ta_common.job_configuration import DefaultFields as df
from time import time
import sys
import traceback
from ta_common.controllers.temporal_trends import TIME_ID

DVH_TIMELINES = False
BY_REGION_ONE = "by_region"
SPARK_LINE = "geo_spark_line"

'''
    To make references to the pos and neg on the mn and mp arrays more readable
'''
NEG = 1
POS = 0


class GeoCodeNotAcceptableException(BaseException):
    '''
        Thrown if a bad geo json region code is passed.

    '''
    pass


class GeoController(object):
    '''

        Aggregates data for the geo entities.

    '''

    def __init__(self, pmongo, table, job_index, build_query, interval_obj, topic_id_filter, show_me):
        '''
            @param pmongo: A Parallel Mongo Object 
            @param table: A grove collection 
            @param job_index: The job configuration dictionary 
            @param build_query:  The method to build the query
            @param interval_boject:  The Interval object that determines the time ranges and max and min scope
            @param topic_id_filter: TopicIdQuery type that determines which topics to filter by in an AND or OR operation.
            @param show_me: a ShowMe class type use to know what views to render for the UI
        '''
        self.table = table
        self.job_index = job_index
        self.build_query = build_query
        self.pmongo = pmongo
        self.interval_obj = interval_obj
        self.show_me = show_me
        self.TopicFilter = topic_id_filter

    def generate_geo_data_query(self, geo_type):
        self.arr = []

        tt = time()

        if self.show_me.regions:
            self.arr.extend(self.__generate_by_region_geo_query(geo_type))
        if self.show_me.heatmap:
            self.arr.extend(self.__generate_by_lat_lon_square_geo_query())

        if DVH_TIMELINES:
            ss = time()
            print "[dvh-time] 0 geo %s %s %s" % (tt, ss, (ss - tt))

    def generate_geo_data_process_answer(self):
        agg_result = self.pmongo.receive(self.arr)

        if self.show_me.regions:
            by_region = self.__generate_by_region_geo_process_answer(
                agg_result)
        if self.show_me.heatmap:
            by_lat_lon = self.__generate_by_lat_lon_square_geo_process_answer(
                agg_result)

        if self.show_me.regions and self.show_me.heatmap:
            return_obj = {
                "regions": by_region,
                "heatmap": by_lat_lon
            }
            return return_obj 
        
        if self.show_me.regions:
            return {"regions": by_region}

        if self.show_me.heatmap:
            return {"heatmap": by_lat_lon}

#     def generate_geo_data(self, geo_type):
#
#         by_region = self.__generate_by_region_geo(geo_type)
#
#         by_lat_lon_squares = self.__generate_by_lat_lon_square_geo()
#
#         return {
#                     "result" : by_region,
#                     "result_coords" : by_lat_lon_squares
#             }

    def __generate_by_region_geo_query(self, geo_type):
        '''

            Aggregates the positive and negative sentiment, the latitude and longitude is averaged over the set of 
            documents that share the same region.  And the number of documents that fall on the region slots is 
            included as count (c)

            The first aggregation query sorts based on the positive sentiment to get the $first document that has 
            the highest positive sentiment for the group.  This query also aggregates other values.

            The second aggregation query sort based on the negative sentiment to get the $first document with the 
            lowest negative sentiment (because it is negative).

        Example to run the query from the mongo shell:

        db['1453304747469_1K_lulu_2016_1_20_10_45am'].aggregate(
            [
                { '$group' : { 
                        '_id' : '$geo_json.region_code', 
                        'name' : { '$first' : '$geo_json.region_code'},
                        'p' : { '$sum' : '$pos' },
                        'n' : { '$sum' : '$neg' },
                        'la': { '$avg' : '$lat'},
                        'lo': { '$avg' : '$lon' },
                        'c' : { '$sum' : 1 }
                    } 
                }
            ]
            );
        '''
        self.geo_type = geo_type
        geo_json_geo_code = self.__check_valid_geo_code(geo_type)

        project = { df.epoch_date: 1, 
                   df.geo_country : 1 , 
                   df.geo_region:1, 
                
                   df.positive: 1,
                   df.negative: 1
                   , df.latitude : 1 , 
                   df.longitude : 1 }
        if self.TopicFilter:
            lst = []
            for topic_id in self.TopicFilter.lst:
                lst.append( "$%s.%s%s" % ( df.topic_weight_doc, df.topic_id_prefix, topic_id  ) ) 
            project['agg_score'] = { '$add': lst }
            project["%s.%s%s" % (df.topic_weight_doc, df.topic_id_prefix, topic_id ) ] = 1
        else:
            lst = []
            for topic_id in range( self.job_index.topics ) :
                lst.append( "$%s.%s%s" % ( df.topic_weight_doc, df.topic_id_prefix, topic_id  ) ) 
            project['agg_score'] = { '$add' : lst }     
            project["%s.%s%s" % (df.topic_weight_doc, df.topic_id_prefix, topic_id ) ] = 1

        group = {
            '_id': geo_json_geo_code,
            #'name': {'$first': '$%s' % df.geo_region}, #'$geo_json.region_code'},
            'country': {'$first': "$%s" % df.geo_country},
            'regions': {'$addToSet': "$%s" % df.geo_region},
            'p': {'$sum': '$%s' % df.positive},
            'n': {'$sum': '$%s' % df.negative},
            'la': {'$avg': '$%s' % df.latitude},
            'lo': {'$avg': '$%s' % df.longitude},
            'mp': {'$first': {'p': '$%s' % df.positive, 'n': '$%s' % df.negative}},
            'mn': {'$last': {'p': '$%s' % df.positive, 'n': '$%s' % df.negative}},
            'agg_score' : { '$sum' : '$agg_score' },
            'c': {'$sum': 1}
        }
        
        '''
            Sum the lda score for each topic on this geo code 
        '''
        for topic_id in range( self.job_index.topics ):
            group["weight_%s"% topic_id] = {"$sum": "$%s.%s%s" % (df.topic_weight_doc, df.topic_id_prefix, topic_id )}
            
        '''
            Sort by sentiment to pick the mp and mn numbers
        '''
        sort_sent = SON([(df.sentiment, -1)])
        '''
            The UI want the result sorted by the lda score
        '''
        sort_mp = SON([('agg_score', -1)])

        '''
            This query does not add the time restriction because we want to get the geo information for all
            the geo data.  But the spack line query will restrict the data to only that which has the
            temporal information
        '''
        query = self.build_query({})

        aggregation = [
            {"$match": query}, { '$project' : project  }, {'$sort': sort_sent }, {'$group': group}, {'$sort': sort_mp}]

        # print 'aggregation'
        # print aggregation

        self.pmongo.aggregate(BY_REGION_ONE, aggregation)

        # The second aggregatin is to get the minimum negative document for
        # each geo location and its positive and negative values.
        # We the merge the two results.

        if self.interval_obj:
            '''
                The geo spark line shows the time bins where the geo is mentioned by the 
                documents
            '''
            project_list = self.interval_obj.getIntervalRangesProjectionDictionary()
            
            query_list = []
            
            for start_date, end_date, number, proj in project_list:
                project = { TIME_ID : proj , df.epoch_date: 1, df.geo: 1, df.positive: 1, df.negative: 1}

                # TemporalTrends.get_time_id(self.interval_obj)}
                spark_id = {"time": self.interval_obj.getTimeIdName()}
                spark_id.update(geo_json_geo_code)
                spark_line_group = {
                    "_id": spark_id,
                    "sum": {"$sum": 1},
                    "e_min": {"$min": "$%s" % df.epoch_date}
                }
    
                '''
                    Make sure only documents with temporal information are used.
                '''
                query = self.build_query({})
                if '$and' in query:
                    query['$and'].append(
                        {df.epoch_date: {'$exists': True, '$ne': None}})
                else:
                    query.update(
                        {'$and': [ {df.epoch_date: {'$exists': True, '$ne': None}}] } )
                
                '''This query helps keep the counts correct from the list of projections '''
                query.update( { df.epoch_date : {  '$gte' : start_date, '$lt' : end_date } } )
                
                query_list.append( [
                    {"$match": query},
                    {"$project": project},
                    {'$group': spark_line_group}] )
                
            self.pmongo.multi_aggregate(SPARK_LINE, query_list)

            return [(BY_REGION_ONE, 1), (SPARK_LINE, 1)]
        else:
            return [(BY_REGION_ONE, 1)]

    def __generate_by_region_geo_process_answer(self, response):

        result = response[BY_REGION_ONE][0][1]

        spark = None
        if self.interval_obj:
            spark_line_groups = response[SPARK_LINE][0][1]

            spark = {}
            for doc in spark_line_groups['result']:

                d_id = doc['_id']

                l_geo_type = 'country' if self.geo_type == 'coords' else self.geo_type

                if l_geo_type in d_id:
                    geo_id_doc = d_id[l_geo_type]
                    try:
                        if not doc['e_min']:
                            print "skiping null e_min ", doc['e_min']
                            continue

                        index = self.interval_obj.getBucketId(doc['e_min'])

                        if geo_id_doc in spark:
                            spark[geo_id_doc][index] = doc['sum']
                        else:
                            spark[geo_id_doc] = [0] * \
                                self.interval_obj.getBucketCount()
                            spark[geo_id_doc][index] = doc['sum']
                    except IndexError as e:
                        try:
                            print " d_id['time'] %s size:  %s " % (d_id['time'], len(spark[geo_id_doc]))
                        except:
                            pass

                        traceback.print_exc(file=sys.stdout)

                else:
                    try:
                        if not doc['e_min']:
                            print "skiping null e_min ", doc['e_min']
                            continue

                        index = self.interval_obj.getBucketId(doc['e_min'])

                        # print "index " , index

                        if None in spark:
                            spark[None][index] = doc['sum']
                        else:
                            spark[None] = [0] * \
                                self.interval_obj.getBucketCount()
                            spark[None][index] = doc['sum']

                    except IndexError as e:
                        traceback.print_exc(file=sys.stdout)

        res = []

        global_unknown = None

        for i in result['result']:
            # _id will be none for global_suro use case .  Global Suro is "Show
            # Unknown Regions Only" for the whole job
            the_id = None

#             print "doc"
#             print i
            is_global_unknown = False

            if self.geo_type == 'country':
                the_id = i['_id'][self.geo_type]
                country = the_id  # this is the country if geo_type is country
                if country in CountryReverseMap:
                    i['display_name'] = CountryReverseMap[country]
                    i['_id'] = the_id

                    if i['regions']:
                        reg_with_name = []
                        for reg in i['regions']:
                            if reg:
                                reg_with_name.append(
                                    [reg, ReverseRegion.get(reg, "")])
                            else:
                                reg_with_name.append(["%s_suro" % country, ""])
                        i['regions'] = reg_with_name

                else:
                    '''
                        If we have no country id, this is the global suro record
                    '''
                    i['display_name'] = "Unknown"
                    i['_id'] = 'global_suro'

                    global_unknown = self.merge_global_unknow(
                        global_unknown,  i)
                    is_global_unknown = True

            elif self.geo_type == 'coords':
                the_id = i['_id']['country']
                if i['_id']:
                    country = i['_id']['country']
                    region = None
                    if 'region' in i['_id']:
                        region = i['_id']['region']

                    if country:
                        country_name = CountryReverseMap[country]
                    else:
                        country_name = 'Unknown'
                        country = 'global_suro'

                    region_name = "unknown"
                    if region:
                        region_name = ReverseRegion[region]
                        i['display_name'] = "%s, %s" % (
                            region_name, country_name)
                    else:
                        i['display_name'] = "Unknown, %s" % country_name

#                     if not region and country:
#                         '''
#                             If we have a country code, but no region, this is a unknown region record.
#                         '''
#                         i['_id'] = '%s_suro' % country
#                     else:
                    i['_id'] = country
                else:
                    '''
                        If we have not Id, this is a global suro record
                    '''
                    i['display_name'] = 'Unknown'
                    i['_id'] = 'suro'
            elif self.geo_type == 'region':

                # print "geo record: " , i

                if self.geo_type in i['_id']:
                    the_id = i['_id'][self.geo_type]
                else:
                    # this is an assertion.  this should not show up on the ui
                    the_id = '??'

                #print "The geo region id : ", i['_id']

                if i['_id']:
                    country = i['_id']['country']
                    region = None
                    if 'region' in i['_id']:
                        region = i['_id']['region']
                    else:
                        pass  # print "no region"

                    if country:
                        country_name = CountryReverseMap[country]
                    else:
                        country_name = 'global_suro'

                        global_unknown = self.merge_global_unknow(
                            global_unknown,  i)
                        is_global_unknown = True

                    region_name = "unknown"
                    if region:
                        region_name = ReverseRegion[region]
                        i['display_name'] = "%s, %s" % (
                            country_name, region_name)
                    else:
                        i['display_name'] = country_name

                    if not region and country:
                        '''
                            If we have a country code, but no region, this is a unknown region record.
                        '''
                        i['_id'] = '%s_suro' % country
                    else:
                        i['_id'] = the_id
                else:
                    '''
                        If we have not Id, this is a global suro record
                    '''
                    i['display_name'] = 'Unknown'
                    i['_id'] = 'global_suro'

                    global_unknown = self.merge_global_unknow(
                        global_unknown,  i)
                    is_global_unknown = True

            if the_id in kGeoCentroid:
                # lon first, second index is latitude
                centroid = kGeoCentroid[the_id]
                i['la'] = centroid[1]
                i['lo'] = centroid[0]

            if spark and the_id in spark:
                i['b'] = {
                    'min': min(spark[the_id]), "bins": spark[the_id], 'max': max(spark[the_id])}

            i['mp'] = [i['mp']['p'], i['mp']['n']]
            i['mn'] = [i['mn']['p'], i['mn']['n']]
            
            i['weight' ] = []
            for topic_id in range( self.job_index.topics): 
                i['weight'].append( i['weight_%s' % topic_id ] ) 
                del i['weight_%s' % topic_id]
                

            if not is_global_unknown:
                res.append(i)

        '''
            We merge the unknows that fall on two different groups.
        '''
        if global_unknown:
            res.append(global_unknown)

        res = sorted(res, key=lambda r: r[ 'agg_score'], reverse=True)

        return res

    def merge_global_unknow(self, current, merge_object):
        '''
            Merges the two global unknowns.  Even though they have different Id's, they belong to 
            the same category

            One has the id None, and the other has the id global_suro.

            TODO: All the internal constants to this object should be replaced with a constant variable
        '''
        # print "These are the two unkonw objects"
        # print current
        # print merge_object

        if not current:
            return merge_object
        else:

            mn = current['mn']
            if mn[NEG] > merge_object['mn']['n']:
                mn = [merge_object['mn']['p'], merge_object['mn']['n']]

            mp = current['mp']
            if mp[POS] < merge_object['mp']['p']:
                mp = [merge_object['mp']['p'], merge_object['mp']['n']]

            regions = []
            regions.extend(current['regions'])
            regions.extend(merge_object['regions'])

            '''
                Only one of the two suro's contains the spark line
            '''
            spark = []
            if 'b' in current:
                spark = current['b']

            if 'b' in merge_object and spark:
                for v in range(len(spark)):
                    '''
                        It assumes the current object and the merge object both have time bins of the same size
                    '''
                    spark[v] += merge_object['b'][v]
            elif not spark and 'b' in merge_object:
                spark = merge_object['b']

            # print "Current"
            # print current

#             import pprint
#
#             pprint.pprint(  current )
#
#             pprint.pprint( merge_object )

            if current['lo'] and merge_object['lo']:
                lo = (current['lo'] + merge_object['lo']) / 2
            elif current['lo']:
                lo = current['lo']
            elif merge_object['lo']:
                lo = merge_object['lo']
            else:
                lo = 0.0

            if current['la'] and merge_object['la']:
                la = (current['la'] + merge_object['la']) / 2
            elif current['la']:
                la = current['la']
            elif merge_object['la']:
                la = merge_object['la']
            else:
                la = 0.0

            return {'c': sum([current['c'], merge_object['c']]),
                    'display_name': 'Unknown',
                    'la': la,
                    'lo': lo,
                    'mn': mn,
                    'n': sum([current['n'], merge_object['n']]),
                    'regions': regions,
                    'p': sum([current['p'], merge_object['p']]),
                    'mp': mp,
                    'b': spark,
                    'country': None,
                    '_id': 'global_suro'}

    def __generate_by_lat_lon_square_geo_query(self):
        '''
            Aggregates the positive and negative sentiment.  The lat and long are averaged over the documents that
            share the regions.  This is similar to __generage_by_region_geo, but this one aggregates on an area of
            lat: 0.1 by lon: 0.1 squares which is around 11 km^2 blocks. 


        Example to run the query from the mongo shell:

        db['57624f6ead1d007be6909370'].aggregate([
        {
            '$group': {
              '_id': {
                'lat': {
                  '$subtract': [
                    {'$multiply': [{'$arrayElemAt': ['$_geo.json.coordinates', 1]}, 10]},
                            {'$mod': [{'$multiply': [{'$arrayElemAt': ['$_geo.json.coordinates', 1]}, 10]}, 1]}
                  ]},
                    'lon': {
                  '$subtract': [
                    {'$multiply': [{'$arrayElemAt': ['$_geo.json.coordinates', 0]}, 10]},
                            {'$mod': [{'$multiply': [{'$arrayElemAt': ['$_geo.json.coordinates', 0]}, 10]}, 1]}
                ]}
            },
                'c': {'$sum': 1},
                'la': {'$avg': {'$arrayElemAt': ['$_geo.json.coordinates', 1]}},
                'lo': {'$avg': {'$arrayElemAt': ['$_geo.json.coordinates', 0]}},
                'n': {'$sum': '$neg'},
                'p': {'$sum': '$pos'}
          }
        }
        ]);
        '''

        group = {
            '_id': self.__get_lat_long_id(),
            'p': {'$sum': '$%s' % df.positive},
            'n': {'$sum': '$%s' % df.negative},
            'la': {'$avg': {'$arrayElemAt': ["$%s" % df.coordinates, 1]}}, # '$%s' % df.latitude},
            'lo': {'$avg': {'$arrayElemAt': ["$%s" % df.coordinates, 0]}}, # '$%s' % df.longitude},
            'region': {'$first':
                       {'country': '$%s' % df.geo_country,
                        'region': '$%s' % df.geo_region}
                       },
            'mp': {'$first': {'p': '$%s' % df.positive, 'n': '$%s' % df.negative}},
            'mn': {'$last': {'p': '$%s' % df.positive, 'n': '$%s' % df.negative}},
            'c': {'$sum': 1}
        }

        sort_mp = {df.sentiment: -1}

        '''
            This should fix cases whith documents that do not have the required 
            group key
        '''
        match = self.build_query(
            {df.latitude: {'$exists': True}, df.longitude: {'$exists': True}})

        self.pmongo.aggregate("by_lat_lon",
                              [{"$match": match}, {'$group': group}, {'$sort': sort_mp}])

        return [('by_lat_lon', 1)]

    def __generate_by_lat_lon_square_geo_process_answer(self, response):
        result = response['by_lat_lon'][0][1]
        res = []
        for i in result['result']:
            if 'la' not in i or 'lo' not in i or i['la'] == None or i['lo'] == None:
                print "Error: la or lo are not float values or not present"
                continue
            i['_id'] = "%.1f_%.1f" % (i['la'], i['lo'])

            if i['region']:

                the_id = None

                i['display_name'] = "unknown"

                if i['region']['region']:
                    the_id = i['region']['region']
                    if the_id in ReverseRegion:
                        i['display_name'] = ReverseRegion[the_id]

                if i['region']['country']:
                    the_id = i['region']['country']
                    if the_id in CountryReverseMap:
                        i['display_name'] += ", %s" % CountryReverseMap[the_id]

                i['mp'] = [i['mp']['p'], i['mp']['n']]
                i['mn'] = [i['mn']['p'], i['mn']['n']]

            res.append(i)
        return res

    def __get_lat_long_id(self):
        '''
            Returns an aggregation key instruction that create a compound key made up of the 
            latitude and the longitude.  The lat and the long are rounded to the nearest 
            decimal to create lat-long squares
        '''

        lat = {'$arrayElemAt': ["$%s" % df.coordinates, 1]}
        lon = {'$arrayElemAt': ["$%s" % df.coordinates, 0]}

        return {
            'lat': {'$subtract': [
                {'$multiply': [lat, 10]},
                {'$mod': [{'$multiply': [lat, 10]}, 1]}
            ]},
            'lon': {'$subtract': [
                {'$multiply': [lon, 10]},
                {'$mod': [{'$multiply': [lon, 10]}, 1]}
            ]}

        }

    def __check_valid_geo_code(self, geo_type):
        if geo_type != "coords" \
            and geo_type != "country" \
                and geo_type != "region":
            raise GeoCodeNotAcceptableException("geo_type: %s " % geo_type)
        elif geo_type == 'coords' or geo_type == "country":
            # "$geo_json.country_code"}
            return {"country": "$%s" % df.geo_country}
        elif geo_type == 'region':
            return {
                "country": "$%s" % df.geo_country,  # $geo_json.country_code",
                "region": "$%s" % df.geo_region  # "$geo_json.region_code"
            }
