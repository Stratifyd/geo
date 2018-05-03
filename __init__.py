#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  @copyright: Unpublished Copyright (c) 2013-2018 [STRATIFYD, INC]
  @license: All Rights Reserved
  @note: All information contained herein is, and remains the property of
    Stratifyd Inc DBA Taste Analytics LLC ("COMPANY"). The intellectual and
    technical concepts contained herein are proprietary to COMPANY and may be
    covered by U.S. and Foreign Patents, patents in process, and are protected
    by trade secret or copyright law. Dissemination of this information or
    reproduction of this material is strictly forbidden unless prior written
    permission is obtained from COMPANY. Access to the source code contained
    herein is hereby forbidden to anyone except current COMPANY employees,
    managers or contractors who have executed confidentiality and
    non-disclosure agreements explicitly covering such access.
"""

from ta_common.relational_object.analyses import Analyses
from ta_common.taste_config_helper import TasteConf

from engine.injection import MaxmindMixin
from mapping import CentroidUpdateHelper

from semi_interactive import SemiInteractiveModel, ModelWrapper


class GeoModel(ModelWrapper):
    TYPE = 'geo'

    @property
    def verbose(self):
        return self.__analysis.verbose


class GeoTask(SemiInteractiveModel):
    TYPE = 'geo'

    def fetch_analysis(self, grove, msg):
        db = grove[msg['db']]
        fid = msg['analysis_fid']
        return GeoModel(Analyses.fetch(db, fid=fid, current=True,
                                       _protected=False))

    def get_analysis_obj(self, db, _id):
        return GeoModel(Analyses.fetch(db, fid=_id, current=True))

    @staticmethod
    def sv_transform_doc(doc, helper):
        lon, lat = doc.get('lon'), doc.get('lat')
        if lon and lat:
            coords = [lon, lat]
            codes = helper.codify(latitude=lat, longitude=lon)
            if any(codes):
                names = helper.translate(*codes)
            else:
                names = []
            return {'geo_index': {'coordinates': coords,
                                  'codes': codes,
                                  'names': names,
                                  'postcode': doc.get('postcode', '')}}
        else:
            return {'geo_index': {}}

    def setup(self, message):
        pass

    def teardown(self, message):
        pass

    def process(self, msg):
        #legacy = msg['legacy']
        backprop = msg.get('backprop', False)

        taste_conf = TasteConf()
#         grove = Grove(taste_conf.getMongoReplicasUrl())
#
#         db = grove[msg['db']]
#
#         analysis = self.get_analysis_obj(db, msg['analysis_fid'])
#         stream = analysis.get_stream()
#         analysis.status = 50
#         analysis.push_changes(upsert=True)

        helper = CentroidUpdateHelper(
            nominatim_host="",
            _country_geocode=taste_conf.getNominatimCountryGeoJSON(),
            _region_geocode=taste_conf.getNominatimRegionGeoJSON(),
            _phone_geocode=taste_conf.getNominatimPhoneGeoJSON())

        self.log('setting up geo engine')
        geo_engine = MaxmindMixin()
        self.log('geo engine setup')
        for doc in self.analysis.get_documents(created=True, backprop=backprop,
                                               version=msg.get('version'),
                                               extra=self.analysis.fields):
            _id = doc.get('_id')
            geo = geo_engine.res_ip_address(
                doc.get('geo_index.ip_address'), None)

            if geo:
                status = self.engine(
                    _id, analyses=GeoTask.sv_transform_doc(geo, helper))


# class GeoTask(SemiInteractiveTask):
#     def __init__(self):
#         pass
#
#     def get_analysis_obj(self, db, _id):
#         return GeoModel(Analyses.fetch(db, fid=_id, current=True))
#
#     @staticmethod
#     def sv_transform_doc(doc):
#         geo = doc.get('_geo', {})
#         code = doc.get('code', {})
#
#         r = {'address': geo.get('full', ''),
#                 'coordinates': geo.get('json',{}).get('coordinates',[]),
#                 'codes': [code.get('country', ''),
#                           code.get('region', '')],
#                 'names':[code.get('country_name', ''),
#                          code.get('region_name', '')],
#                 'postcode': code.get('postal', '')}
#
#         return {'geo_index': r }
#
#     def execute(self , msg ):
#         taste_conf = TasteConf()
#         grove = Grove(taste_conf.getMongoReplicasUrl())
#
#         db = grove[msg['db']]
#
#         analysis = self.get_analysis_obj(db, msg['analysis_fid'])
#         analysis.status = 50
#         analysis.push_changes(upsert=True)
#
#         geo_engine = Piston.spark(client=grove,
#                                 configuration=taste_conf)
#
#         engine = analysis.spawn_cache_engine()
#
#         for _id, geo in geo_engine.iterprocess_streaming(docs=analysis.get_documents(),
#                                                 subdomain=msg['db'],
#                                                 verbose=analysis.verbose):
#
#             status = engine(_id, analyses=GeoTask.sv_transform_doc(geo))
#
#         engine.flush()
#         analysis.init = True
#         analysis.status = 100
#         analysis.utilized = analysis.stream.version
#         analysis.push_changes(upsert=True)
