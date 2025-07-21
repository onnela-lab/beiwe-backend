from pprint import pprint, pp, pformat
import os
import struct
from collections.abc import Generator
from io import BytesIO
from pprint import pprint
from time import perf_counter as p

import numpy
import pandas
# there are multiple different zstd libraries for python, not counting blosc which has zstd as an option
# but it isn't actually better, and is way more complex to use, and if we used the blosc specific
# bit packing then we can't distribute the data in raw form, we have to decompress it at user
# download time on the server. Sooooo we aren't using blosc.
# among pyzstd, zstandard, and zstd, pyzstd is the fastest and has decent extra configuration options.
import pyzstd
from numpy.typing import NDArray


# this is the ZSTD compression level. values of 1,2,3,4 are reasonably fast and still give excellent
# compression. of the tested accelerometer data. Higher values get very slow without adding much
# compression. Sometimes 2 provides better compression than 3 and 4 on raw csv data



"""
####### As numpy arrays 
# CURRENT RESULTS with compression level of 2 and dfast strategy:
note: any times are times for the script as a whole so includes the file-to-binary conversion time
plus python startup.
note: the full size of the data compressed here is actually 530.946MB - that is the size of the text
data representation, I think commas?


lossless conversion - 1 32 bit int, 3 64 bit float conversion:
to-binary-ratio * zstd-ratio:  0.343 / 0.324 = 0.111 total ratio
total compressed ratio: 0.111  915.211MB -> (313.949MB) -> 101.724MB

lossy conversion - 1 32 bit int, 3 32 bit float conversion - 8.400s:
to-binary-ratio * zstd-ratio:  0.196 / 0.463 = 0.091 total ratio
total compressed ratio: 0.091 915.211MB -> (179.399MB) -> 83.042MB

level 5, lazy2 - 
to-binary-ratio * zstd-ratio:  0.196 / 0.421 = 0.083 total ratio
total compressed ratio: 0.083 915.211MB -> (179.399MB) -> 75.534MB

# 10
level 10, lazy2
to-binary-ratio * zstd-ratio:  0.196 / 0.413 = 0.081 total ratio
total compressed ratio: 0.081 915.211MB -> (179.399MB) -> 74.159MB

# Very good compression, probably fast enough - 0m21.666s = 
level 10, btopt - still solidly fast
to-binary-ratio * zstd-ratio:  0.196 / 0.396 = 0.078 total ratio
total compressed ratio: 0.078 915.211MB -> (179.399MB) -> 71.12MB

level 10, btultra - slower
to-binary-ratio * zstd-ratio:  0.196 / 0.401 = 0.079 total ratio
total compressed ratio: 0.079 915.211MB -> (179.399MB) -> 71.858MB

# 20
level 20, lazy2  (actually still pretty fast, faster than 10 btultra and 10 btopt)
to-binary-ratio * zstd-ratio:  0.196 / 0.409 = 0.08 total ratio
total compressed ratio: 0.08 915.211MB -> (179.399MB) -> 73.309MB

level 20, btlazy2
to-binary-ratio * zstd-ratio:  0.196 / 0.402 = 0.079 total ratio
total compressed ratio: 0.079 915.211MB -> (179.399MB) -> 72.068MB

#  This could be viable for a really high compression
level 20, btopt - getting slower
to-binary-ratio * zstd-ratio:  0.196 / 0.376 = 0.074 total ratio
total compressed ratio: 0.074 915.211MB -> (179.399MB) -> 67.482MB
#

level 20, btultra - slow
to-binary-ratio * zstd-ratio:  0.196 / 0.374 = 0.073 total ratio
total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 67.135MB

level 20, btultra2 - slow
to-binary-ratio * zstd-ratio:  0.196 / 0.373 = 0.073 total ratio
total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 66.886MB

# 22
level 22, btopt - actually kinda quick, not better than its 20 test tho
to-binary-ratio * zstd-ratio:  0.196 / 0.376 = 0.074 total ratio
total compressed ratio: 0.074 915.211MB -> (179.399MB) -> 67.481MB

level 22, btultra
to-binary-ratio * zstd-ratio:  0.196 / 0.374 = 0.073 total ratio
total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 67.133MB

level 22, btultra2 - oh its slow. real slow. - 47.368s
to-binary-ratio * zstd-ratio:  0.196 / 0.373 = 0.073 total ratio
total compressed ratio: 0.073 915.211MB -> (179.399MB) -> 66.885MB


####################################################################################################
#  Update

- added some options to run on parquet using pyarrow and fastparquet implementations
of the parquet format.
- built crappy fp xor compression. unfortunately it does not actually improve compression.

Parquet doesn't really get us anything
- it dousn

# sanity check, disabled the xoring - yeah zstd compresses better

# 32 bit - this might have had deltas on whatever
zstd compression level: 10
np binary ratio / zstd ratio: 0.196 * 0.396 = 0.078
915.211MB -> (179.399MB) -> 71.12MB
~
pyarrow ratio: 0.418 (179.399MB -> (75.043MB) -> 65.797MB)
pyarrow zstd ratio: 0.367, combined ratio: 0.072
fastparquet ratio: 0.555  (179.399MB -> (99.574MB) -> 74.597MB)
fastparquet zstd ratio, combined: 0.416 | 0.082


# 64 bit - this had deltas on
zstd compression level: 10
np binary ratio / zstd ratio: 0.392 * 0.25 = 0.098
915.211MB -> (358.799MB) -> 89.7MB
~
pyarrow ratio: 0.279 (358.799MB -> (100.16MB) -> 86.582MB)
pyarrow zstd ratio: 0.241, combined ratio: 0.095
fastparquet ratio: 0.371  (358.799MB -> (133.177MB) -> 102.899MB)
fastparquet zstd ratio, combined: 0.287 | 0.112


# 64 bit but I also turned off xyz deltas, retained time deltas
zstd compression level: 10
np binary ratio / zstd ratio: 0.392 * 0.205 = 0.08
915.211MB -> (358.799MB) -> 73.513MB

pyarrow ratio: 0.237 (358.799MB -> (84.925MB) -> 69.925MB)
pyarrow zstd ratio: 0.195, combined ratio: 0.076
fastparquet ratio: 0.323  (358.799MB -> (115.845MB) -> 76.52MB)
fastparquet zstd ratio, combined: 0.213 | 0.084

"""

ERROR_SEP = "\n=====================Error details:======================\n"

NaN = float("NaN")

def rnd(x: int|float) -> float:
    return round(x, 3)

def megs(size: int|float) -> float:
    return rnd(size / 1024 / 1024)


# we need to measure speed and compression ratio, this is just a global that we can stick stuff in
class m:
    start_time = 0.0
    line_count = 0
    total_to_primitives_compute_time = 0.0
    total_conversion_time = 0.0
    total_to_binary_compute_time = 0.0
    total_uncompressed = 0
    total_to_binary_bytes = 0
    total_zstd_compressed_size = 0
    zstd_compression_time = 0.0
    fpzip_size = 0
    
    adjusted_uncompressed = 0


class pm:
    data_import_time = 0.0
    data_uncompressed = 0
    
    pyarrow_size = 0
    pyarrow_export_time = 0.0
    pyarrow_zstd_size = 0.0
    pyarrow_zstd_time = 0.0
    
    fastparquet_size = 0
    fastparquet_export_time = 0.0
    fastparquet_zstd_size = 0.0
    fastparquet_zstd_time = 0.0


# we need to measure speed and compression ratio, this is just a global that we can stick stuff in
class m_xor:
    start_time = 0.0
    line_count = 0
    total_to_primitives_compute_time = 0.0
    total_conversion_time = 0.0
    total_to_binary_compute_time = 0.0
    total_uncompressed = 0
    total_to_binary_bytes = 0
    total_zstd_compressed_size = 0
    zstd_compression_time = 0.0
    fpzip_size = 0
    
    adjusted_uncompressed = 0



####################################################################################################



####################################################################################################
# script expects a folder of data-downloaded-from-beiwe-form in at a location
DATA_FOLDER = "./private/data"

# weird csv parsing code stolen from other parts of the codebase
def csv_to_list(file_contents: bytes) -> Generator[list[bytes], None, None]:
    if b"\n" not in file_contents:
        raise Exception("File contents do not contain newlines, cannot parse as CSV.")
    
    m.total_uncompressed += len(file_contents)
    
    line_iterator = isplit(file_contents)
    _ = b",".join(next(line_iterator))
    return line_iterator

def isplit(source: bytes) -> Generator[list[bytes], None, None]:
    start = 0
    while True:
        idx = source.find(b"\n", start)
        if idx == -1:
            yield source[start:].split(b",")
            return
        yield source[start:idx].split(b",")
        start = idx + 1


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


####################################################################################################


# uses pyzstd python library - it has an extra runtime option, richmem_compress, that is faster than
# zstd. Current backend uses zstd but could easily be swapped.
def compress_zstd(some_bytes):
    t1 = p()
    output = pyzstd.RichMemZstdCompressor(ZSTD_KWARGS).compress(some_bytes)  # type: ignore
    t = p() - t1
    m.zstd_compression_time += t
    m.total_zstd_compressed_size += len(output)
    return output


def compress_xor(some_bytes):
    t1 = p()
    output = pyzstd.RichMemZstdCompressor(ZSTD_KWARGS).compress(some_bytes)  # type: ignore
    t = p() - t1
    m_xor.zstd_compression_time += t
    m_xor.total_zstd_compressed_size += len(output)
    return output


def compress_pyarrow(some_bytes):
    t1 = p()
    output = pyzstd.RichMemZstdCompressor(ZSTD_KWARGS).compress(some_bytes)  # type: ignore
    t2 = p()
    duration = t2 - t1
    pm.pyarrow_zstd_time += duration
    pm.pyarrow_zstd_size += len(output)
    return output


def compress_fastparquet(some_bytes):
    t1 = p()
    output = pyzstd.RichMemZstdCompressor(ZSTD_KWARGS).compress(some_bytes)  # type: ignore
    t2 = p()
    duration = t2 - t1
    pm.fastparquet_zstd_time += duration
    pm.fastparquet_zstd_size += len(output)
    return output


####################################################################################################

BIT_TYPES_64 = [
    ("timestamp", numpy.uint64), ("x", numpy.float64), ("y", numpy.float64), ("z", numpy.float64),
]

# numpy types for each column
ACCELEROMETER_DTYPES = [
    ("timestamp", numpy.uint64), ("x", numpy.float64), ("y", numpy.float64), ("z", numpy.float64),
]

# string types required because it has to consume the values correctly
# PANDAS_ACCELEROMETER_DTYPES = [
#     ("timestamp", numpy.uint64), ("UTC time", numpy.str_), ("accuracy", numpy.str_),
#     ("x", numpy.float64), ("y", numpy.float64), ("z", numpy.float64),
# ]
PANDAS_ACCELEROMETER_DTYPES = [
    ("timestamp", numpy.uint64), ("UTC time", numpy.str_), ("accuracy", numpy.str_),
    ("x", numpy.float32), ("y", numpy.float32), ("z", numpy.float32),
]
import pyarrow

ARROW_DTYPES = [
    ("timestamp", "uint64[pyarrow]"), ("UTC time", "string[pyarrow]"), ("accuracy", "string[pyarrow]"),
    ("x", "float32[pyarrow]"), ("y", "float32[pyarrow]"), ("z", "float32[pyarrow]"),
]

# ignore the accuracy column for now
ACCELEROMETER_PANDAS = [["timestamp", "x", "y", "z"], [numpy.uint64, numpy.float64, numpy.float64, numpy.float64]]
XYZ_DTYPES = [("x", numpy.float64), ("y", numpy.float64), ("z", numpy.float64)]

# delta compression types for pandas parquet (parquet)
# ACCELEROMETER_TDELTA_DTYPES = [
#     ("timestamp", numpy.uint32), ("x", numpy.float32), ("y", numpy.float32), ("z", numpy.float32),
# ]
ACCELEROMETER_TDELTA_DTYPES = [
    ("timestamp", numpy.uint32), ("x", numpy.float64), ("y", numpy.float64), ("z", numpy.float64),
]

XOR_DTYPES = [
    ("timestamp", numpy.uint64), ("x", numpy.float64), ("y", numpy.float64), ("z", numpy.float64),
]
XOR_DTYPES = [
    ("timestamp", numpy.uint32), ("x", numpy.float32), ("y", numpy.float32), ("z", numpy.float32),
]


####################################################################################################



# ZSTD_COMPRESSION_LEVEL = 2
# ZSTD_OPT = pyzstd.Strategy.dfast  # use at 2
ZSTD_COMPRESSION_LEVEL = 10
ZSTD_OPT = pyzstd.Strategy.btopt  # use at 10
ZSTD_KWARGS = {
    pyzstd.CParameter.nbWorkers: 1,
    pyzstd.CParameter.compressionLevel: ZSTD_COMPRESSION_LEVEL,
    pyzstd.CParameter.strategy: ZSTD_OPT,
}

PARQUET_KWARGS = {  # options: snappy, gzip, lz4, zstd, None, (and brotli, but it is too slow)
    "index": False, "compression": "snappy",
}

csv_import_settings = dict(
    low_memory=False,               # low memory might have performance impact
    header=0,                       # header, names, are probably redundant... wwhatever
    names=["timestamp", "UTC time", "accuracy", "x", "y", "z"],
    dtype=PANDAS_ACCELEROMETER_DTYPES, # type: ignore
    usecols=[0,3,4,5],              # you have to specify the columns you want to use
    delimiter=",",   lineterminator="\n",
)

# csv_import_settings = dict(
#     header=0,                       # header, names, are probably redundant... wwhatever
#     names=["timestamp", "UTC time", "accuracy", "x", "y", "z"],
#     dtype=PANDAS_ACCELEROMETER_DTYPES, # type: ignore
#     usecols=["timestamp", "x", "y", "z"],              # you have to specify the columns you want to use
#     delimiter=",",
    
#     engine="pyarrow",
# )


####################################################################################################


def do_it_parquet(in_data: bytes, path: str):
    try:
        return _do_it_parquet(in_data, path)
    except (NotImplementedError, Exception):
        print("Error processing file:", path)
        print(ERROR_SEP)
        pprint(in_data[:1000].split(b"\n"), width=1000)  # print the first 1000 bytes of the file
        print(ERROR_SEP)
        raise


def _do_it_parquet(in_data: bytes, path: str):
    pm.data_uncompressed += len(in_data)
    
    t_bytes_start = p()
    bytes_io = BytesIO(in_data)
    
    df = pandas.read_csv(bytes_io, **csv_import_settings)  # type: ignore
    t_bytes_end = p();  pm.data_import_time += t_bytes_end - t_bytes_start
    
    # pyarrow parquet export
    t_pyarrow_start = p()
    
    par1 = df.to_parquet(engine='pyarrow', **PARQUET_KWARGS)  # type: ignore
    t_pyarrow_end = p()
    pm.pyarrow_size += len(par1);  pm.pyarrow_export_time += t_pyarrow_end - t_pyarrow_start
    compress_pyarrow(par1)
    
    # fastparquet parquet export
    t_fastparquet_start = p()
    par2 = df.to_parquet(engine='fastparquet',**PARQUET_KWARGS)  # type: ignore
    t_fastparquet_end = p()
    pm.fastparquet_size += len(par2)
    pm.fastparquet_export_time += t_fastparquet_end - t_fastparquet_start
    compress_fastparquet(par2)


def print_stuff_parquet():
    orig_size = pm.data_uncompressed
    uncompressed_megs = megs(pm.data_uncompressed)
    
    pyarrow_megs = megs(pm.pyarrow_size)
    fastparquet_megs = megs(pm.fastparquet_size)
    
    zstd_pyarrow_megs = megs(pm.pyarrow_zstd_size)
    zstd_fastparquet_megs = megs(pm.fastparquet_zstd_size)
    
    pyarrow_ratio = rnd(pm.pyarrow_size / orig_size)
    fastparquet_ratio = rnd(pm.fastparquet_size / orig_size)
    
    zstd_pyarrow_ratio = rnd(pm.pyarrow_zstd_size / pm.pyarrow_size)
    zstd_fastparquet_ratio = rnd(pm.fastparquet_zstd_size / orig_size)
    
    # print("pyarrow compression ratio:", pyarrow_ratio,
    #       f"{uncompressed_megs}MB -> ({pyarrow_megs}MB) -> {zstd_pyarrow_megs}MB",
    #       "\npyarrow zstd compression ratio:", zstd_pyarrow_ratio)
    
    # print("fastparquet compression ratio:", fastparquet_ratio,
    #       f"{uncompressed_megs}MB -> ({fastparquet_megs}MB) -> {zstd_fastparquet_megs}MB"
    #       "\nfastparquet zstd compression ratio:", zstd_fastparquet_ratio)
    
    # total_import_time = rnd(pm.data_import_time)  # needs to be non-zero
    # if total_import_time < 0.001:
        # print("insufficient import time, not printing speed")
    # import_mbps = rnd(uncompressed_megs / pm.data_import_time)
    # export_pyarrow_mbps = rnd(pyarrow_megs / pm.pyarrow_export_time)
    # export_fastparquet_mbps = rnd(fastparquet_megs / pm.fastparquet_export_time)
    
    # total_pyarrow_time = rnd(pm.pyarrow_export_time)
    # total_fastparquet_time = rnd(pm.fastparquet_export_time)
        
        # print("import time:", total_import_time, "s", "import speed:", import_mbps, "MB/s")
        # print("pyarrow time:", total_pyarrow_time, "s", "export speed:", export_pyarrow_mbps, "MB/s")
        # print("fastparquet time:", total_fastparquet_time, "s", "export speed:", export_fastparquet_mbps, "MB/s")
    
    # print it like this:
    # pyarrow ratio: 0.552 (179.399MB -> (99.086MB) -> 80.076MB)
    # pyarrow zstd ratio: 0.446, combined ratio: 0.087
    # fastparquet ratio: 1.015  (179.399MB -> (182.149MB) -> 70.026MB)
    # fastparquet zstd ratio, combined: 0.39 | 0.077
    
    # ug this wrong.... ok whatever
    print(f"pyarrow ratio: {pyarrow_ratio} ({uncompressed_megs}MB -> ({pyarrow_megs}MB) -> {zstd_pyarrow_megs}MB)")
    print(f"pyarrow zstd ratio: {zstd_pyarrow_ratio}, combined ratio: {rnd(pyarrow_ratio * zstd_pyarrow_ratio)}")
    print(f"fastparquet ratio: {fastparquet_ratio}  ({uncompressed_megs}MB -> ({fastparquet_megs}MB) -> {zstd_fastparquet_megs}MB)")
    print(f"fastparquet zstd ratio, combined: {zstd_fastparquet_ratio} | {rnd(fastparquet_ratio * zstd_fastparquet_ratio)}")
    print()


####################################################################################################


def do_it_parquet_delta(in_data: NDArray, path, size: int):
    try:
        return _do_it_parquet_delta(in_data, size)
    except (NotImplementedError, Exception):
        print("Error processing file:", path)
        print(ERROR_SEP)
        pprint(in_data[:1000], width=1000)  # print the first 1000 bytes of the file
        print(ERROR_SEP)
        raise


def _do_it_parquet_delta(in_data: NDArray, size: int):
    pm.data_uncompressed += size
    df = pandas.DataFrame(in_data)
    
    # pyarrow parquet export
    t_pyarrow_start = p()
    par1 = df.to_parquet(engine='pyarrow', **PARQUET_KWARGS)  # type: ignore
    t_pyarrow_end = p()
    pm.pyarrow_size += len(par1)
    pm.pyarrow_export_time += t_pyarrow_end - t_pyarrow_start
    compress_pyarrow(par1)
    
    # fastparquet parquet export
    t_fastparquet_start = p()
    par2 = df.to_parquet(engine='fastparquet', **PARQUET_KWARGS)  # type: ignore
    t_fastparquet_end = p()
    pm.fastparquet_export_time += t_fastparquet_end - t_fastparquet_start
    pm.fastparquet_size += len(par2)
    compress_fastparquet(par2)


def print_stuff_delta():
    orig_raw = m.total_uncompressed
    delta_size_raw = pm.data_uncompressed
    uncompressed_megs = megs(delta_size_raw)
    
    # pyarrow ratio for original data
    pyarrow_megs = megs(pm.pyarrow_size)
    zstd_pyarrow_megs = megs(pm.pyarrow_zstd_size)
    
    pyarrow_delta_ratio = rnd(pm.pyarrow_size / delta_size_raw)
    zstd_vs_just_pyarrow_delta_ratio = rnd(pm.pyarrow_zstd_size / delta_size_raw)
    total_zstd_pyarrow_delta_ratio = rnd(pm.pyarrow_zstd_size / orig_raw)
    # print("pyarrow + delta compression ratio:", pyarrow_delta_ratio,
    #       f"{uncompressed_megs}MB -> ({pyarrow_megs}MB) -> {zstd_pyarrow_megs}MB",
    #       "\npyarrow zstd delta compression ratio:", zstd_vs_just_pyarrow_delta_ratio,
    #       "\ntotal combined compression ratio:", total_zstd_pyarrow_delta_ratio)
    
    # fastparquet ratio for original data
    fastparquet_megs = megs(pm.fastparquet_size)
    zstd_fastparquet_megs = megs(pm.fastparquet_zstd_size)
    
    fastparquet_delta_ratio = rnd(pm.fastparquet_size / delta_size_raw)
    zstd_vs_fastparquet_delta_ratio = rnd(pm.fastparquet_zstd_size / delta_size_raw)
    total_zstd_fastparquet_delta_ratio = rnd(pm.fastparquet_zstd_size / orig_raw)
    # print("fastparquet + delta compression ratio:", fastparquet_delta_ratio,
    #       f"{uncompressed_megs}MB -> ({fastparquet_megs}MB) -> {zstd_fastparquet_megs}MB",
    #       "\nfastparquet zstd delta compression ratio:", zstd_vs_fastparquet_delta_ratio,
    #       "\ntotal combined compression ratio:", total_zstd_fastparquet_delta_ratio)
    # print()
    
    print(f"pyarrow ratio: {pyarrow_delta_ratio} ({uncompressed_megs}MB -> ({pyarrow_megs}MB) -> {zstd_pyarrow_megs}MB)")
    print(f"pyarrow zstd ratio, combined: {zstd_vs_just_pyarrow_delta_ratio} | {rnd(pyarrow_delta_ratio * zstd_vs_just_pyarrow_delta_ratio)}")
    print(f"fastparquet ratio: {fastparquet_delta_ratio}  ({uncompressed_megs}MB -> ({fastparquet_megs}MB) -> {zstd_fastparquet_megs}MB)")
    print(f"fastparquet zstd ratio, combined: {zstd_vs_fastparquet_delta_ratio} | {rnd(fastparquet_delta_ratio * zstd_vs_fastparquet_delta_ratio)}")
    print()


# I need the print statements that make the above output look like this:
# np binary ratio / zstd ratio: 0.392 * 0.36 = 0.141
# 915.211MB -> (358.799MB) -> 129.219MB


####################################################################################################


def do_delta_numpy(in_data: bytes, path: str):
    # line_iterator is a generator that yields a list of bytes (undecoded strings of utf8
    # characters, I think) for each line in the csv
    all_columns: list[tuple[bytes, ...]] = []  # assemble the data out of the bytes
    
    # _time_str is the iso time string, we don't need it.
    t_iterator_start = p()
    all_columns = [
        (timestamp, x, y, z) for timestamp, _time_str, _accuracy, x, y, z in csv_to_list(in_data)
    ]
    
    # ok the original length is actually this
    m.adjusted_uncompressed += sum(sum((len(timestamp), len(x), len(y), len(z),)) for timestamp, x, y, z in all_columns)
    m.adjusted_uncompressed += 4  # commas, newline
    t2 = p()
    array = numpy.array(all_columns, dtype=ACCELEROMETER_DTYPES)
    
    # this code computes the values as deltas! this actually works! we a compression improvement!
    # NOTE: this is technically wrong on the timestamp column, we want "0" to be the hour-start
    # value, not the first element value, but that doesn't matter for compression.
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
    
    t_end = p()
    m.total_to_binary_compute_time += t_end - t_iterator_start
    m.total_to_primitives_compute_time += t2 - t_iterator_start
    m.total_conversion_time = t_end - t2
    
    # TODO: array.data as a memoryview len() returns a smaller value than the bytes. do we care?
    operative_data = array.data.tobytes()  # operative_data = array.data  # nope
    
    m.total_to_binary_bytes += len(operative_data)
    compress_zstd(operative_data)  # does its own timing and stats
    
    return array, len(operative_data)  # return the bytes, not the array, so we can use it in the next step
    
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


def print_stuff_numpy():
    if m.adjusted_uncompressed:
        adjusted_megs = megs(m.adjusted_uncompressed)
    if not m.total_uncompressed:
        print("no data in m.total_uncompressed 1")
        exit()
    binary_compression_ratio = rnd(m.total_to_binary_bytes / m.total_uncompressed)
    just_zstd_compression_ratio = rnd(m.total_zstd_compressed_size / m.total_to_binary_bytes)
    total_compression_ratio = rnd(m.total_zstd_compressed_size / m.total_uncompressed)
    total_size = megs(m.total_uncompressed)
    binary_compressed_size = megs(m.total_to_binary_bytes)
    total_compressed_size = megs(m.total_zstd_compressed_size)
    
    # I need the print statements that make the above look like this:
    # np binary ratio / zstd ratio: 0.392 * 0.36 = 0.141
    # 915.211MB -> (358.799MB) -> 129.219MB
    msg = f"np binary ratio / zstd ratio: {binary_compression_ratio} * {just_zstd_compression_ratio} = {total_compression_ratio}"
    
    if m.adjusted_uncompressed:
        adjusted_megs = megs(m.adjusted_uncompressed)
        msg += f"\n{total_size}MB -> ({adjusted_megs}MB) -> ({binary_compressed_size}MB) -> {total_compressed_size}MB\n"
    else:
        msg += f"\n{total_size}MB -> ({binary_compressed_size}MB) -> {total_compressed_size}MB\n"
    print(msg)



####################################################################################################

def fxor_double(a, b):
    rtrn = []
    a = struct.pack('d', a)
    b = struct.pack('d', b)
    for ba, bb in zip(a, b):
        rtrn.append(ba ^ bb)
    return struct.unpack('d', bytes(rtrn))[0]


def fxor_float(a, b):
    rtrn = []
    a = struct.pack('f', a)
    b = struct.pack('f', b)
    for ba, bb in zip(a, b):
        rtrn.append(ba ^ bb)
    return struct.unpack('f', bytes(rtrn))[0]


def do_delta_and_xor_numpy(in_data: bytes, path: str):
    # line_iterator is a generator that yields a list of bytes (undecoded strings of utf8
    # characters, I think) for each line in the csv
    all_columns: list[tuple[bytes, ...]] = []  # assemble the data out of the bytes
    
    # _time_str is the iso time string, we don't need it.
    t_iterator_start = p()
    all_columns = [
        (timestamp, x, y, z) for timestamp, _time_str, _accuracy, x, y, z in csv_to_list(in_data)
    ]
    
    # ok the original length is actually this
    m_xor.adjusted_uncompressed += sum(sum((len(timestamp), len(x), len(y), len(z),)) for timestamp, x, y, z in all_columns)
    t2 = p()
    array = numpy.array(all_columns, dtype=BIT_TYPES_64)
    # print(array[:10])
    
    # this code computes the values as deltas! this actually works! we a compression improvement!
    # NOTE: this is technically wrong on the timestamp column, we want "0" to be the hour-start
    # value, not the first element value, but that doesn't matter for compression.
    # array["timestamp"][1:] = numpy.diff(array["timestamp"])
    # array["timestamp"][0] = 0 # set to zero so we can go down to 32 bits
    # array["x"][1:] = numpy.diff(array["x"])
    # array["y"][1:] = numpy.diff(array["y"])
    # array["z"][1:] = numpy.diff(array["z"])
    
    # change the data type to 32 bits, this is lossy but it is a lot smaller
    # array = array.astype(dtype=XOR_DTYPES, casting="same_kind")
    
    # x, y, z = array["x"], array["y"], array["z"]  # viewz into the array data, so we can modify it
    # new_x = []
    # new_y = []
    # new_z = []
    # for i in range(1, len(array)):
    #     # xor each value with the previous value
    #     # print(x[i], x[i-1])
    #     # print(y[i], y[i-1])
    #     # print(z[i], z[i-1])
    #     # new_x.append(fxor_double(x[i], x[i-1]))
    #     # new_y.append(fxor_double(y[i], y[i-1]))
    #     # new_z.append(fxor_double(z[i], z[i-1]))
    #     new_x.append(fxor_float(x[i], x[i-1]))
    #     new_y.append(fxor_float(y[i], y[i-1]))
    #     new_z.append(fxor_float(z[i], z[i-1]))
        
    #     # print(x[i], y[i], z[i])
    #     pass
    # for i in range(1, len(array)):
    #     # set the new values in the array
    #     x[i] = new_x[i-1]
    #     y[i] = new_y[i-1]
    #     z[i] = new_z[i-1]
    # print(array[:10])
    
    operative_data = array.data.tobytes()
    
    # uncomment the following code to print the data in a human readable format of the bits/bytes
    
    # # bit width x 4 items x 2 hex chars per byte divided by 8 bits per byte
    # width = 64*4*2 / 8  # 64 bit width
    # ######################## width = 32*4*2 / 8  # 32 bit width
    # # assert int(width) == width, f"Width is not an integer, got {width}"
    # width = int(width)
    # as_hex = operative_data.hex()
    # for i in range(0, len(as_hex), width):
    #     row = as_hex[i:i+width]
    #     # print("=")
    #     t,x,y,z = row[:16], row[16:32], row[32:48], row[48:64]  # these are hex strings
    #     t = int(t, 16)
    #     x_bin = bin(int(x, 16))  # lacks padding
    #     y_bin = bin(int(y, 16))  # lacks padding
    #     z_bin = bin(int(z, 16))  # lacks padding
    #     # add padding to the binary strings
    #     x_bin = x_bin[2:].zfill(64)  # remove the "0b" prefix
    #     y_bin = y_bin[2:].zfill(64)  # remove the "0b" prefix
    #     z_bin = z_bin[2:].zfill(64)  # remove the "0b" prefix
    
    #     # x_float = struct.unpack('d', bytes.fromhex(x))[0]
    #     # y_float = struct.unpack('d', bytes.fromhex(y))[0]
    #     # z_float = struct.unpack('d', bytes.fromhex(z))[0]
    #     # print(t, x, y, z)
    #     # print(hex(t), f"0x{x}", f"0x{y}", f"0x{z}")
    #     # print(t, x_float, y_float, z_float)
    #     print(bin(t), x_bin, y_bin, z_bin)
    #     if i > 500:
    #         exit()
    # # assert (w:= len(as_hex) % width*8) == 0, f"Data length is not a multiple of the width., got remainder {w}"
    
    t_end = p()
    m_xor.total_to_binary_compute_time += t_end - t_iterator_start
    m_xor.total_to_primitives_compute_time += t2 - t_iterator_start
    m_xor.total_conversion_time = t_end - t2
    
    # TODO: array.data as a memoryview len() returns a smaller value than the bytes. do we care?
    operative_data = array.data.tobytes()  # operative_data = array.data  # nope
    
    m_xor.total_to_binary_bytes += len(operative_data)
    compress_xor(operative_data)  # does its own timing and stats
    
    return array, len(operative_data)  # return the bytes, not the array, so we can use it in the next step


def print_stuff_numpy_xor():
    
    binary_compression_ratio = rnd(m_xor.total_to_binary_bytes / m.total_uncompressed)
    just_zstd_compression_ratio = rnd(m_xor.total_zstd_compressed_size / m_xor.total_to_binary_bytes)
    total_compression_ratio = rnd(m_xor.total_zstd_compressed_size / m.total_uncompressed)
    total_size = megs(m.total_uncompressed)
    binary_compressed_size = megs(m_xor.total_to_binary_bytes)
    total_compressed_size = megs(m_xor.total_zstd_compressed_size)
    msg = f"np binary ratio / zstd ratio: {binary_compression_ratio} * {just_zstd_compression_ratio} = {total_compression_ratio}"
    
    if m_xor.adjusted_uncompressed:
        adjusted_megs = megs(m_xor.adjusted_uncompressed)
        msg += f"\n{total_size}MB -> ({adjusted_megs}MB) -> ({binary_compressed_size}MB) -> {total_compressed_size}MB\n"
    else:
        msg += f"\n{total_size}MB -> ({binary_compressed_size}MB) -> {total_compressed_size}MB\n"
    print(msg)


####################################################################################################

def benchmark_pandas_csv_import():
    """
    This function benchmarks the pandas CSV import performance.
    It reads a CSV file and measures the time taken to import it.
    """
    import time
    import pandas as pd
    
    csv_file_path = "path/to/your/csvfile.csv"  # replace with your actual CSV file path
    start_time = time.time()
    
    df = pd.read_csv(csv_file_path, **csv_import_settings)  # type: ignore
    
    end_time = time.time()
    print(f"CSV import took {end_time - start_time:.2f} seconds.")


####################################################################################################


def main():
    m.start_time = p()
    
    for path, data in iterate_all_files():
        print("zstd compression level:", ZSTD_COMPRESSION_LEVEL)
        # uncomment to run with the delta feeding into parquet
        # delta_data, size = do_delta_numpy(data, path)
        # do_it_parquet_delta(delta_data, path, size)
        
        # uncomment to run with raw data feeding into parquet
        # do_it_numpy(data, path)
        do_it_parquet(data, path)
        
        print_stuff_parquet()
        # print_stuff_delta()
        # print_stuff_numpy()
        
        # delta_data, size = do_delta_and_xor_numpy(data, path)
        # do_it_parquet_delta(delta_data, path, size)
        # print_stuff_numpy_xor()
        # print()
        # print_stuff_delta()

main()
