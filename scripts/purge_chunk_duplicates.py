# modify python path so that this script can be targeted directly but still import everything.
import imp as _imp
from os.path import abspath as _abspath
_current_folder_init = _abspath(__file__).rsplit('/', 1)[0]+ "/__init__.py"
_imp.load_source("__init__", _current_folder_init)


from datetime import datetime
from db.data_access_models import ChunksRegistry

keys = ('chunk_hash','chunk_path','data_type','is_chunkable','study_id','survey_id','time_bin','user_id')

duplicate_chunk_paths = set()

def duplicate_chunk_path_severity(chunk_path):
    """ Compare contents of all chunks matching this path, blow up if any values are different.
        (They should all be identical.) """
    collisions = {key: set() for key in keys}
    for key in keys:
        for chunk in ChunksRegistry(chunk_path=chunk_path):
            collisions[key].add(chunk[key])

    for key, collision in collisions.items():
        if len(collision) > 1:
            print collisions
            raise Exception("encountered bad duplicate chunk requiring manual purge.")
            
t1 = datetime.now()
for chunk in ChunksRegistry.iterator():
    if ChunksRegistry.count(chunk_path=chunk['chunk_path']) > 1:
        if chunk['chunk_path'] in duplicate_chunk_paths:
            print chunk['chunk_path']
        duplicate_chunk_paths.add(chunk['chunk_path'])
        duplicate_chunk_path_severity(chunk['chunk_path'])

t2 = datetime.now()
print "discovered %s duplicate chunks in %s seconds" % (len(duplicate_chunk_paths), (t2-t1).total_seconds())

for path in duplicate_chunk_paths:
    while ChunksRegistry.count(chunk_path=path) > 1:
        ChunksRegistry(chunk_path=path)[0].remove()
        print "purging", path, ChunksRegistry.count(chunk_path=path)

