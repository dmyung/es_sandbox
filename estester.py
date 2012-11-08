import simplejson
import rawes
import os

ES_HOST = 'vmlocal'
ES_PORT = 9200
ES_TYPE = 'testtype'

def get_es():
    return rawes.Elastic('%s:%s' % (ES_HOST, ES_PORT))

index_mappings = {
'nothing': None,
'nodate': {'date_detection': False},
#typed...
}

def main():
    files = os.listdir('.')
    files.sort()

    for index, mapping  in index_mappings.items():
        es = get_es()
        if es.head(index):
            es.delete(index)
        es.put(index)
        if mapping is not None:
            es.put("%s/%s/_mapping" % (index, ES_TYPE), data=mapping)
        for x in files:
            if x.endswith('.json'):
                filenum = int(x[0:3])
                success=True
                with open(x, 'r') as fin:
                    res = es.put("%s/%s/%d" % (index, ES_TYPE, filenum), data=simplejson.loads(fin.read()))
                    if res.get('status',None) == 400:
                        success=False
                print "%s, %s => %s" % (index, x, 'SUCCESS' if success else
                'FAIL')

if __name__ == "__main__":
    main()
