import simplejson
import rawes
import os
import uuid
import random

#Utils for testing date time parsing events

ES_HOST = 'vmlocal'
ES_PORT = 9200
ES_TYPE = 'testtype'

def get_es():
    return rawes.Elastic('%s:%s' % (ES_HOST, ES_PORT))


         
date_format_arr = ["yyyy-MM-dd",
        #"date_time_no_millis",
        #                      'date_optional_time',
        "yyyy-MM-dd'T'HH:mm:ssZZ",
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        "yyyy-MM-dd'T'HH:mm:ss'Z'",
        "yyyy-MM-dd'T'HH:mm:ssZ",
        "yyyy-MM-dd'T'HH:mm:ssZZ'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd' 'HH:mm:ss",
        "yyyy-MM-dd' 'HH:mm:ss.SSSSSS",
        "mm/dd/yy' 'HH:mm:ss",
] 
#https://github.com/elasticsearch/elasticsearch/issues/2132
#elasticsearch Illegal pattern component: t
#no builtin types for || joins
formats_string = '||'.join(date_format_arr)


index_mappings = {
        'nothing': None,
        'nodate': {'nodate': {'date_detection': False}},
        'dynamic_date': {
            'dynamic_date': {
                "date_formats" : date_format_arr,
                }
            },
        'static_formats': {
            'static_formats': {
                "properties": {
                    "consistent_date": {
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_0": {
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_1": {
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_2": {
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_3": {
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_4": {
                        #"format": "dateOptionalTime",
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_5": {
                        #format: dateOptionalTime
                        #"format": "dateOptionalTime",
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_6": {
                        #"format": "dateOptionalTime",
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_7": {
                        #"format": "dateOptionalTime",
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_8": {
                        #"format": "dateOptionalTime",
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_9": {
                        #format: dateOptionalTime
                        #"format": "dateOptionalTime",
                        "format": formats_string,
                        "type": "date"
                        },
                    "unique_10": {
                            #"format": "dateOptionalTime",
                            "format": formats_string,
                            "type": "date"
                            },
                    "unique_11": {
                            #"format": "dateOptionalTime",
                            "format": formats_string,
                            },
                    "unique_12": {
                            #"format": "dateOptionalTime",
                            "format": formats_string,
                            }

                    }
                }
            },
    'dynamic_all': {
            'dynamic_all':  {
                "dynamic_templates" : [ 
                    {
                        "store_generic" : {
                            "match" : "*",
                            "mapping": { "store" : "yes" }
                            }
                        }
                    ]
                }
            }
    }

date_examples = [
        '2012-02-28T12:12:08+00:00Z',#0
        '2012-02-28T12:12:08Z', #1
        '2012-02-28T12:12:08.464-05', #2 xform date_modified
        '2011-01-01', #3
        '2012-11-15 18:41:55', #4 couch active tasks
        '2011-09-16 12:21:13.254286', #5 timeend crazy digits
        '1353022915', #6
        1353022915, #7
        'Sat Nov 17 03:51:25 2012', #8 apache error example
        'Fri, 16 Nov 2012', #9 github example
        None, #10
        '', #11
        '11/10/09 21:23:43', #12 old xform submissions
        ]

def main():
    import hashlib
    files = os.listdir('.')
    files.sort()

    for index, mapping  in index_mappings.items():
        print "================= Index %s ====================" % index
        es = get_es()
        if es.head(index):
            es.delete(index)
        es.put(index)
        if mapping is not None:
            print "putting mapping for %s" % index
            print es.put("%s/%s/_mapping" % (index, index), data=mapping)
        curr_mapping = None
        for ix, date_string in enumerate(date_examples):
            doc_data = {"unique_%s" % ix: date_string}

            #for jx, other in enumerate(date_examples):
                #if jx == ix:
                    #continue
                #doc_data['%d_to_%d' % (ix, jx)] = date_string
                #doc_data['%d_from_%d' % (ix, jx)] = other

            res = es.put("%s/%s/%d" % (index, index, ix),
                    data=doc_data)
            if res.get('status',None) == 400:
                success=False
            else:
                success=True
            check_mapping = es.get('%s/%s/_mapping?pretty=true' % (index, index))
            print "#### %s, %s => %s" % (index, date_string, 'SUCCESS' if success else 'FAIL,%s' % res['error'])
            if simplejson.dumps(check_mapping) != simplejson.dumps(curr_mapping):
                curr_mapping = check_mapping
                #print curr_mapping
            print ""
    for i, d in enumerate(date_examples):
        print "%d: %s" % (i, d)

if __name__ == "__main__":
    main()
