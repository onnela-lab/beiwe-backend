from os.path import abspath as _abspath
from sys import path as _path


_one_folder_up = _abspath(__file__).rsplit('/',2)[0]
_path.insert(1, _one_folder_up)
