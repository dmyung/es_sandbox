import simplejson
import rawes
import os

ES_HOST = 'localhost'
ES_PORT = 9200
ES_TYPE = 'testtype'

def get_es():
    return rawes.Elastic('%s:%s' % (ES_HOST, ES_PORT))

index_mappings = {
    'nothing': None,
    'nodate': {'nodate': {'date_detection': False}},
    'dynamic_date': {
        'dynamic_date': {
            "dynamic_date_formats" : ["yyyy-MM-dd", "dd-MM-yyyy",
                "date_optional_time", ] #"" doesn't work
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
            print es.put("%s/%s/_mapping" % (index, index), data=mapping)
        curr_mapping = None
        for x in files:
            if x.endswith('.json'):
                filenum = int(x[0:3])
                success=True
                with open(x, 'r') as fin:
                    res = es.put("%s/%s/%d" % (index, index, filenum), data=simplejson.loads(fin.read()))
                    if res.get('status',None) == 400:
                        success=False
                    check_mapping = es.get('%s/%s/_mapping?pretty=true' % (index, index))
                print "#### %s, %s => %s" % (index, x, 'SUCCESS' if success else
                'FAIL,%s' % res['error'])

                if simplejson.dumps(check_mapping) != simplejson.dumps(curr_mapping):
                    curr_mapping = check_mapping
                    print curr_mapping
                print ""

if __name__ == "__main__":
    main()
