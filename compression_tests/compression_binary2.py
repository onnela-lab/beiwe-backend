import os
from time import perf_counter
from typing import Generator, List, Tuple

import numpy

# there are two different zstd libraries for python, not counting blosc which has zstd as an option
# but it isn't actually better, and is way more complex to use, and if we used the blosc specific
# bit packing then we can't distribute the data in raw form, we have to decompress it at user
# download time on the server. Sooooo we aren't using blosc.
import pyzstd


# this is the ZSTD compression level. values of 1,2,3,4 are reasonably fast and still give excellent
# compression. of the tested accelerometer data. Higher values get very slow without adding much
# compression. Sometimes 2 provides better compression than 3 and 4.
COMPRESSION_LEVEL = 3


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
    
    measurements.total_bytes_current_file = len(file_contents)
    measurements.total_uncompressed += measurements.total_bytes_current_file
    
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
    total_bytes_current_file = 0
    total_uncompressed = 0
    total_to_binary_bytes = 0
    total_zstd_compressed_size = 0
    zstd_compression_time = 0.0

NaN = float("NaN")

# numpy types for each column
ACCELEROMETER_DTYPES = [
    ("timestamp", numpy.uint64,),  # TODO: can we do a 48 bit int? (or ideally a 43 bit int?)
    ("accuracy", numpy.float64,),
    ("x", numpy.float64,),
    ("y", numpy.float64,),
    ("z", numpy.float64,),
]

# ACCELEROMETER_DTYPES = [
#     ("timestamp", numpy.uint64,),  # TODO: can we do a 48 bit int? (or ideally a 43 bit int?)
#     # ("accuracy", numpy.float64,),
#     ("x", numpy.float32,),
#     ("y", numpy.float32,),
#     ("z", numpy.float32,),
# ]


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
        do_it_as_numpy(data, path)
        print()


# FIXME: ok it looks like the pure-python implementation I started with got some bit rot and
# probably doesn't work anymore. This is fine, we want the numpy version anyway.
def do_it_as_python(data: bytes, path: str):
    # 3.8, total_compute_time: 7.72735 time_per_row: 0.0000006572 run time: 19.73537
    # pyston, total_compute_time: 3.98756 time_per_row: 0.0000003392 run time: 9.99690
    # 3.11, total_compute_time: 5.47458 time_per_row: 0.0000004656 run time: 15.70931
    
    header, line_iterator = csv_to_list(data)
    output = []
    for timestamp, time_string, accuracy, x, y, z in line_iterator:
        measurements.line_count += 1
        # string_representation_1 = timestamp.decode().lower(), x.decode().lower(), y.decode().lower(), z.decode().lower(), accuracy.decode().lower() if accuracy == b"unknown" else accuracy.decode().lower()+".0"
        t1 = perf_counter()
        timestamp = int(timestamp)
        accuracy = NaN if accuracy == b"unknown" else float(accuracy)
        x = float(x)
        y = float(y)
        z = float(z)
        t2 = perf_counter()
        output.append((timestamp, accuracy, x, y, z, ))
        measurements.total_to_binary_compute_time += (t2 - t1)
        # string_representation_2 = repr(timestamp), repr(x), repr(y), repr(z), "unknown" if accuracy is NaN else repr(accuracy)
        # print("accuracy.is_integer", accuracy.is_integer())
        # assert string_representation_1 == string_representation_2, print("", string_representation_1, "\n", string_representation_2)
        # print(" ".join(string_representation_2))


# THIS IS THE BIG FUNCTION THAT DOES THE NUMPY... STUFF

def do_it_as_numpy(in_data: bytes, path: str):
    # line_iterator is a generator that yields a list of bytes (undecoded strings of utf8
    # characters, I think) for each line in the csv
    _header, line_iterator = csv_to_list(in_data)
    
    data: list = []  # assemble the data out of the bytes
    
    # _time_string is the iso time string, we don't need it.    
    # [b'1539395563219', b'2018-10-13T01:52:43.219', b'unknown', b'0.01904296875', b'-0.00531005859375', b'-0.99017333984375']
    t1 = perf_counter()
    for timestamp, _time_string, accuracy, x, y, z in line_iterator:
        # accuracy = NaN if accuracy == b"unknown" else accuracy
        # data.append((timestamp, accuracy, x, y, z))
        data.append((timestamp, x, y, z))
    t2 = perf_counter()
    array = numpy.array(data, dtype=ACCELEROMETER_DTYPES)
    
    numpy_bytes = array.dumps()
    t3 = perf_counter()
    measurements.total_to_binary_compute_time += t3 - t1
    measurements.total_to_primitives_compute_time += t2 - t1
    measurements.total_numpy_conversion_time = t3 - t2
    
    print(
        "array.nbytes:",
        array.nbytes,
        "numpy_dumps_bytes:",
        len(numpy_bytes),
        # "array.data:",
        # len(array.data),
        # "the raw bytes:",
        # len(array.data.tobytes()),
    )
    # print(array)
    # exit()
    
    
    # assert array.nbytes == len(numpy_bytes)
    
    measurements.total_to_binary_bytes += len(numpy_bytes)
    compress(numpy_bytes)  # does its own timing and stuff
    
    # this section of code compares the compressed and then decompressed data to the original data.
    # x = numpy.load(BytesIO(decompress(compressed)), allow_pickle=True)
    # # comparing two rows in the array
    # for y, z in zip(x, stuff):
    #     # print(y, z)
    #     for y1, z1 in zip(y, z):
    #         if numpy.isnan(y1) and numpy.isnan(z1):
    #             continue
    #         assert y1 == z1, (y, y1, z, z1)
    
    binary_compression_ratio = round(measurements.total_to_binary_bytes / measurements.total_uncompressed, 2)
    zstd_compression_ratio = round(measurements.total_zstd_compressed_size / measurements.total_to_binary_bytes, 2)
    total_compression_ratio = round(measurements.total_zstd_compressed_size / measurements.total_uncompressed, 2)
    total_size = round(measurements.total_uncompressed / 1024 / 1024, 2)
    total_compressed_size = round(measurements.total_zstd_compressed_size / 1024 / 1024, 2)
    
    print("total binary compression ratio:", binary_compression_ratio)
    print("total zstd compression ratio:", zstd_compression_ratio)
    print("total compressed ratio:", total_compression_ratio, f"({total_size}MB) -> {total_compressed_size}MB")
    
    # print("total compression time:", measurements.zstd_compression_time, f"({measurements.zstd_compression_time / measurements.total_to_binary_compute_time* 100:.2f}%)")
    # print("total data size:", f"", "MB")
    
    # exit()
    # print("file size:", measurements.total_bytes_current_file)
    # print("stuff.nbytes:", stuff.nbytes)
    # print("compressed size:", final_compressed_size)
    # print("relative bytes count:", f"{stuff.nbytes / measurements.total_bytes_current_file:.5f}")
    # print("relative compressed size:", f"{final_compressed_size / measurements.total_bytes_current_file:.5f}")
    # print("compression time:", f"{measurements.most_recent_zstd_compression_time:.5f}", f"({measurements.most_recent_zstd_compression_time / t * 100:.2f}%)")


# uses pyzstd python library - it has an extra runtime option, richmem_compress, that is faster than
# zstd. Current backend uses zstd but could easily be swapped.
def compress(some_bytes):
    t1 = perf_counter()
    output = pyzstd.RichMemZstdCompressor(
        { # type: ignore
            # pyzstd.CParameter.compressionLevel: COMPRESSION_LEVEL,
            
            pyzstd.CParameter.nbWorkers: 1,
            # pyzstd.CParameter.enableLongDistanceMatching: 1,
            # pyzstd.CParameter.strategy: pyzstd.Strategy.btlazy2,
            # pyzstd.CParameter.strategy: pyzstd.Strategy.btultra2,
            
            pyzstd.CParameter.strategy: pyzstd.Strategy.dfast,
        }
    ).compress(some_bytes)
    
    t = perf_counter() - t1
    measurements.zstd_compression_time += t
    measurements.total_zstd_compressed_size += len(output)
    
    return output


main()
