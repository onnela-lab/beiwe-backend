import os
from time import perf_counter
from typing import Generator, List, Tuple

# import fpzip
import numpy
# there are multiple different zstd libraries for python, not counting blosc which has zstd as an option
# but it isn't actually better, and is way more complex to use, and if we used the blosc specific
# bit packing then we can't distribute the data in raw form, we have to decompress it at user
# download time on the server. Sooooo we aren't using blosc.
# among pyzstd, zstandard, and zstd, pyzstd is the fastest and has decent extra configuration options.
import pyzstd


# this is the ZSTD compression level. values of 1,2,3,4 are reasonably fast and still give excellent
# compression. of the tested accelerometer data. Higher values get very slow without adding much
# compression. Sometimes 2 provides better compression than 3 and 4 on raw csv data
COMPRESSION_LEVEL = 2


"""
#CURRENT RESULTS with compression level of 2 and dfast strategy:
note that any times are times for the script as a whole so includes the file-to-binary conversion time plus python startup

lossless conversion - 1 32 bit int and 3 64 bit float conversion:
  total binary compression ratio: 0.343
  total zstd compression ratio: 0.324
  total compressed ratio: 0.111  915.211MB -> (313.949MB) -> 101.724MB

lossy conversion - 1 32 bit int and 3 32 bit float conversion - 8.400s:
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.463
  total compressed ratio: 0.091 915.211MB -> (179.399MB) -> 83.042MB

but because that is less data it takes less time for a slower zstd compression to run

level 5, lazy2 - 
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.421
  total compressed ratio: 0.083 915.211MB -> (179.399MB) -> 75.534MB

# 10
level 10, lazy2
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.413
  total compressed ratio: 0.081 915.211MB -> (179.399MB) -> 74.159MB

# Very good compression, probably fast enough - 0m21.666s
level 10, btopt - still solidly fast
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.396
  total compressed ratio: 0.078 915.211MB -> (179.399MB) -> 71.12MB
#
  
level 10, btultra - slower
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.401
  total compressed ratio: 0.079 915.211MB -> (179.399MB) -> 71.858MB

# 20
level 20, lazy2  (actually still pretty fast, faster than 10 btultra and 10 btopt)
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.409
  total compressed ratio: 0.08 915.211MB -> (179.399MB) -> 73.309MB

level 20, btlazy2
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.402
  total compressed ratio: 0.079 915.211MB -> (179.399MB) -> 72.068MB

#  This could be viable for a really high compression
level 20, btopt - getting slower
total binary compression ratio: 0.196
  total zstd compression ratio: 0.376
  total compressed ratio: 0.074 915.211MB -> (179.399MB) -> 67.482MB
#
  
level 20, btultra - slow
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.374
  total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 67.135MB

level 20, btultra2 - slow
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.373
  total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 66.886MB

# 22
level 22, btopt - actually kinda quick, not better than its 20 test tho
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.376
  total compressed ratio: 0.074 915.211MB -> (179.399MB) -> 67.481MB

level 22, btultra
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.374
  total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 67.133MB

level 22, btultra2 - oh its slow. real slow. - 47.368s
  total binary compression ratio: 0.196
  total zstd compression ratio: 0.373
  total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 66.885MB

"""




# these two top functions are the pretty optimized iteration code from the beiwe-backend codebase,
# short of migrating the iteration to C or Cython or whatever these have the best characteristics
# for running on our servers. (we prefer low memory overhead to raw speed)
def csv_to_list(file_contents: bytes) -> Tuple[bytes, Generator[List[bytes], None, None]]:
    """ Grab a list elements from of every line in the csv, strips off trailing whitespace. dumps
    them into a new list (of lists), and returns the header line along with the list of rows. """
    
    # This code is more memory efficient than fast by using a generator
    # Note that almost all of the time is spent in the per-row for-loop
    
    # case: the file coming in is just a single line, e.g. the header.
    # Need to provide the header and an empty iterator.
    if b"\n" not in file_contents:
        return file_contents, (_ for _ in ())
    
    measurements.total_uncompressed += len(file_contents)
    
    line_iterator = isplit(file_contents)
    header = b",".join(next(line_iterator))
    header2 = file_contents[:file_contents.find(b"\n")]
    assert header2 == header, f"\n{header}\n{header2}"
    return header, line_iterator


def isplit(source: bytes) -> Generator[list[bytes], None, None]:
    """ Generator version of str.split()/bytes.split() """
    # version using str.find(), less overhead than re.finditer()
    start = 0
    while True:
        # find first split
        idx = source.find(b"\n", start)
        if idx == -1:
            yield source[start:].split(b",")
            return
        
        yield source[start:idx].split(b",")
        start = idx + 1


# we need to measure speed and compression ratio, this is just a global that we can stick stuff in
class measurements:
    start_time = 0.0
    line_count = 0
    total_to_primitives_compute_time = 0.0
    total_numpy_conversion_time = 0.0
    total_to_binary_compute_time = 0.0
    total_uncompressed = 0
    total_to_binary_bytes = 0
    total_zstd_compressed_size = 0
    zstd_compression_time = 0.0
    fpzip_size = 0

NaN = float("NaN")

# numpy types for each column
ACCELEROMETER_DTYPES = [
    ("timestamp", numpy.uint64,),
    # ("accuracy", numpy.float64,),  # currently just not dealing with the accuracy column...
    ("x", numpy.float64,),
    ("y", numpy.float64,),
    ("z", numpy.float64,),
]

XYZ_DTYPES = [
    ("x", numpy.float64),
    ("y", numpy.float64),
    ("z", numpy.float64),
]

ACCELEROMETER_TDELTA_DTYPES = [
    ("timestamp", numpy.uint32,),
    # ("accuracy", numpy.float64,),
    ("x", numpy.float32,),
    ("y", numpy.float32,),
    ("z", numpy.float32,),
]


# this script expects there to be a folder of data-downloaded-from-beiwe-form in a specific folder.
# It only currently looks at the accelerometer data.
DATA_FOLDER = "./private/data"


# this is a generator that yields the path to the file and the file contents as bytes
def iterate_all_files():
    for user_path in os.listdir(DATA_FOLDER):
        # folder structure is folders per user, then per-datastream,
        for user_datastream in os.listdir(f"{DATA_FOLDER}/{user_path}/"):
            # only accelerometer for now
            if user_datastream != "accelerometer":
                continue
            
            user_datastream_path = f"{DATA_FOLDER}/{user_path}/{user_datastream}/"
            for user_datastream_file in os.listdir(user_datastream_path):
                user_datastream_file_path = f"{user_datastream_path}/{user_datastream_file}"
                if not os.path.isdir(user_datastream_file_path):
                    with open(user_datastream_file_path, "rb") as f:
                        data: bytes = f.read()
                        if data: # skip empty files
                            yield user_datastream_file_path, data


def main():
    measurements.start_time = perf_counter()
    print("compression level:", COMPRESSION_LEVEL)
    for path, data in iterate_all_files():
        # do_it_as_python(data, path)
        do_it(data, path)
        print_stuff()


def do_it(in_data: bytes, path: str):
    # line_iterator is a generator that yields a list of bytes (undecoded strings of utf8
    # characters, I think) for each line in the csv
    _header, line_iterator = csv_to_list(in_data)
    
    
    all_fpzip = []  # FPZIP claims to be good with floats, but I'm not seeing it do very well, also it crashes
    all_columns: list = []  # assemble the data out of the bytes
    
    t1 = perf_counter()
    
    # _time_string is the iso time string, we don't need it.
    all_columns = [
        (timestamp, x, y, z)
        # (x, y, z)  # for fpzip tests....
        for timestamp, _time_string, accuracy, x, y, z in line_iterator
    ]
    
    t2 = perf_counter()
    array = numpy.array(all_columns, dtype=ACCELEROMETER_DTYPES)
     
    # fpzip doesn't like the structured array, it wants a float array, so we will proxy as 3
    # separate arrays for testing.  Unfortunately this crashes sometimes on smaller array sizes.
    # measurements.fpzip_size += len(fpzip.compress(array["x"]))
    # measurements.fpzip_size += len(fpzip.compress(array["y"]))
    # measurements.fpzip_size += len(fpzip.compress(array["z"]))
    
    # this code computes the values as deltas! this actually works! we a compression improvement!
    # NOTE: this is technically wrong on the timestamp column, we want "0" to be the hour-start
    # value, not the first element value, but that doesn't matter for compression.
    #
    array["timestamp"][1:] = numpy.diff(array["timestamp"])
    array["x"][1:] = numpy.diff(array["x"])
    array["y"][1:] = numpy.diff(array["y"])
    array["z"][1:] = numpy.diff(array["z"])
    array["timestamp"][0] = 0 # set to zero so we can go down to 32 bits
    
    # More Random tests
    # order - column or row ordering - does nothing for zstd compression ratio.
    # array = array.flatten(order="C")  # flatten does nothing for zstd compression ratio
    
    # data type conversions - slightly more complex than it might seem at first glance.
    
    # the input strings are no of uniform length, so its not a trivial fraction - but it hovers
    # around the expected values.  1 32 bit int and 3 32 bit floats gets you ~0.34x original size.
    
    # NOTE: the less precise types have higher entropy and zstd (or any compression standard) will 
    # not compress them at the same ratio from all 64 to all 32 bits the final output size on 
    # ~900MB of accelerometer data at level 3 compression using dfast mode in pzstd goes from 
    # ~100MB output to ~75MB final calculation.
    array = array.astype(dtype=ACCELEROMETER_TDELTA_DTYPES, casting="same_kind")
    
    t3 = perf_counter()
    measurements.total_to_binary_compute_time += t3 - t1
    measurements.total_to_primitives_compute_time += t2 - t1
    measurements.total_numpy_conversion_time = t3 - t2
    
    # TODO: array.data as a memoryview len() returns a smaller value than the bytes. do we care?
    operative_data = array.data.tobytes()  # operative_data = array.data  # nope
    
    measurements.total_to_binary_bytes += len(operative_data)
    compress(operative_data)  # does its own timing and stats
    
    # TODO: this is broken, fix
    # This section of code compares the compressed and then decompressed data to the original data.
    # x = numpy.load(BytesIO(decompress(compressed)), allow_pickle=True)
    # # comparing two rows in the array
    # for y, z in zip(x, stuff):
    #     # print(y, z)
    #     for y1, z1 in zip(y, z):
    #         if numpy.isnan(y1) and numpy.isnan(z1):
    #             continue
    #         assert y1 == z1, (y, y1, z, z1)


def print_stuff():
    binary_compression_ratio = round(measurements.total_to_binary_bytes / measurements.total_uncompressed, 3)
    zstd_compression_ratio = round(measurements.total_zstd_compressed_size / measurements.total_to_binary_bytes, 3)
    total_compression_ratio = round(measurements.total_zstd_compressed_size / measurements.total_uncompressed, 3)
    fpzip_size = round(measurements.fpzip_size / 1024 / 1024, 3)
    total_size = round(measurements.total_uncompressed / 1024 / 1024, 3)
    binary_compressed_size = round(measurements.total_to_binary_bytes / 1024 / 1024, 3)
    total_compressed_size = round(measurements.total_zstd_compressed_size / 1024 / 1024, 3)
    
    
    print("total binary compression ratio:", binary_compression_ratio)
    print("total zstd compression ratio:", zstd_compression_ratio)
    print(
        "total compressed ratio:", total_compression_ratio,
        f"{total_size}MB -> ({binary_compressed_size}MB) -> {total_compressed_size}MB"
    )
    # print("fpzip size:", fpzip_size, "MB")
    print()



# uses pyzstd python library - it has an extra runtime option, richmem_compress, that is faster than
# zstd. Current backend uses zstd but could easily be swapped.
def compress(some_bytes):
    t1 = perf_counter()
    output = pyzstd.RichMemZstdCompressor(
        { # type: ignore
            # pyzstd.CParameter.compressionLevel: 15,
            # pyzstd.CParameter.compressionLevel: 22,  # gets you virtually nothing
            pyzstd.CParameter.compressionLevel: COMPRESSION_LEVEL,
            
            pyzstd.CParameter.nbWorkers: 1,
            # pyzstd.CParameter.enableLongDistanceMatching: 1,
            
            # these compression levels are known to be decent on the raw csv data
            pyzstd.CParameter.strategy: pyzstd.Strategy.dfast,
            # pyzstd.CParameter.strategy: pyzstd.Strategy.lazy2,
            # pyzstd.CParameter.strategy: pyzstd.Strategy.btlazy2,
            # pyzstd.CParameter.strategy: pyzstd.Strategy.btopt,
            # pyzstd.CParameter.strategy: pyzstd.Strategy.btultra,
            # pyzstd.CParameter.strategy: pyzstd.Strategy.btultra2,
        }
    ).compress(some_bytes)
    
    t = perf_counter() - t1
    measurements.zstd_compression_time += t
    measurements.total_zstd_compressed_size += len(output)
    
    return output


main()
