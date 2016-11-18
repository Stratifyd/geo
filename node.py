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
  @project: Taste Analytics Computation Node-- Geocode Request Handler

  @version: 0.26-sigma
  @updated: November 3, 2016
  @requires: python 2.7.10
"""

from ta_common.field_names import ENV, MC, JS
from time import time
from v2_ta_common.process_node import ProcessNode
from v2_tier3_compute_node.geo.engine import Engine, Piston


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
        start = time()
        self.engine = Piston.spark(client=self.mongo_helper,
                                   configuration=self.taste_conf)
        print "Initialization required %f seconds." % (time() - start)

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
            self.engine.process(config=self.job_configuration,
                                subdomain=self.job_configuration.mongo_db,
                                verbose=ENV.get(ENV.VERBOSE, as_type=int))

    def state(self):
        return {'processed_docs': self.engine.processed}

    def finish_job(self):
        pass

if __name__ == '__main__':
    def get_mapping(piston, config, grove):
        return piston.generate_field_mapping(config)

    def get_example(piston, config, grove):
        mapping = get_mapping(piston, config, grove)
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
            type_, query = piston.remap_documents(doc, mapping)
            if not query:
                continue

            examples.append({"input": query, "type": type_, "_id": doc["_id"]})
            for attempt in range(5):
                geocode = piston.fire(query, type_)

                if query:
                    query = getattr(geocode, "get_%s" % type_)(
                        query=query, attempt=attempt)
                else:
                    break

                if not query:
                    break
                example = examples[-1].setdefault(str(attempt), {})
                example["raw_query"] = query

                server_response = getattr(
                    geocode, "res_%s" % type_)(query, set())
                if server_response:
                    example["raw_result"] = server_response[0]
                else:
                    example["raw_result"] = None

                if geocode.result:
                    example["geo_result"] = geocode.result[0]
                else:
                    example["geo_result"] = None

        return examples

    def rerun_job(piston, config, grove, verbose=True):
        piston.process(config=config, verbose=verbose, subdomain=Ellipsis)

    from sys import argv
    _script, subdomain, job_info, run_type = argv[:4]
    subdomain = subdomain.lower()
    job_info = job_info.lower()
    run_type = run_type.lower()

    from bson import ObjectId
    from pprint import pprint
    from ta_common.mango import Grove
    from ta_common.mango.relational_object.jobs import Jobs
    from ta_common.taste_config_helper import TasteConf

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
        nom = Piston.spark(client=grove, configuration=TasteConf())
        if 'm' in run_type:  # print job conf's mapping
            print "Mapping:"
            pprint(get_mapping(nom, job, grove))
            print
        if 'e' in run_type:  # print example from job
            print "Examples:"
            for example in get_example(nom, job, grove):
                pprint(example)
                print
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
