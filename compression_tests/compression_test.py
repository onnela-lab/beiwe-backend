from copy import copy
import os
from itertools import chain
from random import shuffle
from time import perf_counter
from typing import DefaultDict

import blosc2
import pyzstd
from scipy.stats import f
import zstd
from blosc2 import Filter

from zstandard import ZstdCompressor, ZstdDecompressor

# from libs.utils.dev_utils import GlobalTimeTracker, p  # , print_type


blosc2.set_releasegil(False)
blosc2.set_nthreads(1)

BLOSC_FILTER_MAP = {
    0: "NOFILTER  ",
    1: "SHUFFLE   ",
    2: "BITSHUFFLE",
    3: "DELTA     ",
    4: "TRUNC_PREC",
    32: "NDCELL",
    33: "NDMEAN",
    35: "BYTEDELTA",
    36: "INT_TRUNC",
}

BLOSC_FILTERS = [
    Filter.NOFILTER,  # old comment! appears to be wrong now everything else gets half compression level and throughput 
    # Filter.SHUFFLE,
    # Filter.BITSHUFFLE,
    # Filter.DELTA,
    # Filter.TRUNC_PREC,  # "lossy"
    # Filter.NDCELL,
    # Filter.NDMEAN,
    # Filter.BYTEDELTA,
    # Filter.INT_TRUNC,
]


BLOSC_CODECS_MAP = {
    blosc2.Codec.BLOSCLZ:  "BLOSCLZ  ",
    blosc2.Codec.LZ4:      "LZ4      ",
    blosc2.Codec.ZSTD:     "ZSTD     ",
    blosc2.Codec.ZLIB:     "ZLIB     ",
    blosc2.Codec.LZ4HC:    "LZ4HC    ",
    blosc2.Codec.NDLZ:     "NDLZ     ",
    blosc2.Codec.ZFP_ACC:  "ZFP_ACC  ",
    blosc2.Codec.ZFP_PREC: "ZFP_PREC ",
    blosc2.Codec.ZFP_RATE: "ZFP_RATE ",
    # blosc2.Codec.LZ4HC
}

PYZSTD_STRATEGIES = [
    # pyzstd.Strategy.fast,
    pyzstd.Strategy.dfast,
    # pyzstd.Strategy.greedy,
    # pyzstd.Strategy.lazy,
    # pyzstd.Strategy.lazy2,
    # pyzstd.Strategy.btlazy2,
    # pyzstd.Strategy.btopt,
    # pyzstd.Strategy.btultra,
    # pyzstd.Strategy.btultra2,
]

PYZSTD_STRAT_NAMES = {
    pyzstd.Strategy.fast: "fast",
    pyzstd.Strategy.dfast: "dfast",
    pyzstd.Strategy.greedy: "greedy",
    pyzstd.Strategy.lazy: "lazy",
    pyzstd.Strategy.lazy2: "lazy2",
    pyzstd.Strategy.btlazy2: "btlazy2",
    pyzstd.Strategy.btopt: "btopt",
    pyzstd.Strategy.btultra: "btultra",
    pyzstd.Strategy.btultra2: "btultra2",
}

# with open("./private/dictionary", "rb") as f:
#     the_zstd_dictionary = pyzstd.ZstdDict(f.read(), is_raw=True)


def iterate_all_files():
    for user_path in os.listdir("./private/data"):
        for user_datastream in os.listdir(f"./private/data/{user_path}/"):
            if user_datastream != "accelerometer":
            # if user_datastream != "gyro":
            # if user_datastream != "gps":
            # if user_datastream != "wifi":
            # if user_datastream != "texts":
            # if user_datastream != "reachability":
            # if user_datastream != "power_state":
            # if user_datastream != "ios_log":
            # if user_datastream != "identifiers":
            # if user_datastream != "calls":
            # if user_datastream != "app_log":
                continue
            user_datastream_path = f"./private/data/{user_path}/{user_datastream}/"
            for user_datastream_file in os.listdir(user_datastream_path):
                user_datastream_file_path = f"{user_datastream_path}/{user_datastream_file}"
                if not os.path.isdir(user_datastream_file_path):
                    with open(user_datastream_file_path, "rb") as f:
                        data: bytes = f.read()
                        if data:
                            yield user_path, user_datastream, data
                        # yield user_path, user_datastream, b"A" * 400000


def print_stuff(data_stream, running_counts, compress_time):
    print()
    total_size = running_counts[data_stream]
    for k, v in sorted(running_counts.items(), key=lambda x: x[0]):
        if k.endswith("_dec") or k == data_stream:
            continue
        
        decode_time = compress_time[k + "_dec"]
        decode_rate = round(total_size / 1024 / 1024 / decode_time if decode_time else 0, 2)
        compress_rate = round(total_size / 1024 / 1024 / compress_time[k] if compress_time [k] else 0, 2)
        comress_ratio = f"{round(v / total_size * 100, 2)}%"
        compress_seconds = f"{round(compress_time[k], 2)}(sec)"
        print(
            k,
            comress_ratio,
            compress_seconds,
            compress_rate,
            "MB/s encode",
            decode_rate,
            "MB/s decode",
            sep="\t"
        )
    print(f"{(total_size / 1024 / 1024):.2f}", "MB")


def comp_disp(i: int):
    if i < 0:
        return "-0" + str(abs(i))
    elif i < 10:
        return "0" + str(i)
    else:
        return "" + str(i)


def main_blosc():
    # compression level
    # use_dict: randomly crashes with "can't compress data", and it is slower, and it has worse compression
    # typesize: has roughly one one-hundredth of a percent (1/10,000th).  50 is good?
    # block_size: 0 is the baseline, it takes until 16k-32k for any value to meet its performance,
    #  64k starts to beat it, but it takes like 1024*1024+ to reliably beat it. It also achieves better
    #  compression too (18% vs 20%), but decode speed is roughly half of block_size 0 so you have to
    #  take the total.
    # meta: no value appears to make any difference, it always has a moderate variance run-to-run.
    # typesize: same as meta, there is no consistency, variance is too high.
    # ntheads: scales moderately, but if typesize is large it has no effect. when blocksize is 0 it
    #  scales ok (very roughly +50% cpu usage per thread), when blocksize is like 1024*64 it scales
    # very well too.  Base cpu usage of core count 1 is very roughly 115%, so scaling is very weird
    
    running_counts = DefaultDict(int)
    running_time = DefaultDict(float)
    block_base = 1073741888 - 128
    
    for participant_id, data_stream, content in iterate_all_files():
        running_counts[data_stream] += len(content)
        # blosc2
        for comp_level in [1]:  # range(0, 9+1):
            # block_list = list(chain(range(block_base, block_base + 256+1, 8), [0], ))
            # shuffle(block_list)
            for block_size in [0, 1073741888]:  # block_list:
                for typesize in [0]:  # range(1, 256 + 1):
                    for use_dict in [False]:
                        for meta in [0]:  # range(0,127+1,):
                            compress_blosc(data_stream, comp_level, typesize, block_size, use_dict, content,
                            running_time, running_counts, meta)
            print_stuff(data_stream, running_counts, running_time)


def main_zstd():
    # compression level 1 is very fast and gets compression to ~18%, compression 22 gets only ~16%
    # and is absurdly slow
    running_counts = DefaultDict(int)
    running_time = DefaultDict(float)
    for participant_id, data_stream, content in iterate_all_files():
        running_counts[data_stream] += len(content)
        for comp_level in [10, 1]:#range(0, 22+1):
            compress_zstd(data_stream, comp_level, content, running_time, running_counts)
        print_stuff(data_stream, running_counts, running_time)


def main_pyzstd():
    # compression level 1 is very fast and gets compression to ~18%, compression 22 gets only ~16%
    # and is absurdly slow
    running_counts = DefaultDict(int)
    running_time = DefaultDict(float)
    for participant_id, data_stream, content in iterate_all_files():
        running_counts[data_stream] += len(content)
        for comp_level in [16]:  #range(0, 22+1):
            for strat in PYZSTD_STRATEGIES:
                compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, strat)
        print_stuff(data_stream, running_counts, running_time)


def main_zstandard():
    # ZstdCompressor(level=1, threads=1).compress
    # ZstdDecompressor().decompress
    running_counts = DefaultDict(int)
    running_time = DefaultDict(float)
    for participant_id, data_stream, content in iterate_all_files():
        running_counts[data_stream] += len(content)
        for comp_level in [1, 10]:#range(0, 22+1):
            compress_zstandard(data_stream, comp_level, content, running_time, running_counts)
        print_stuff(data_stream, running_counts, running_time)


def main_all():
    running_counts = DefaultDict(int)
    running_time = DefaultDict(float)
    meta = 0  # 126  # is randomly good?
    use_dict = True
    typesize = 8
    block_size = 1024*128*1024 + 64  # this value happens to be good, unclear why.
    
    for participant_id, data_stream, content in iterate_all_files():
        running_counts[data_stream] += len(content)
        
        for comp_level in [0,1,2,3,4,5,6,7,8,9]:
            # if comp_level in [0,1,2,3,4,5]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.fast)
            if comp_level in [0,1,2]:
                compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.dfast)
            # if comp_level in [0,1,2,3,4,5]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.greedy)
            if comp_level in [1,2,4,5,6,7,8,9]:
                compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.lazy)
            # if comp_level in [1,2,4,5,6,7,8,9]:
                # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.lazy2)
            # if comp_level in [1,2,4,5,6,7,8,9]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btlazy2)
            # if comp_level in [1,2,4,5,6,7,8,9]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btopt)
            # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btopt)
            # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btultra)
            # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btultra2)
        
        
        # for blosc_comp_level in [1,2,3,4,5,6,7,8,9]:
        #     compress_blosc(data_stream, blosc_comp_level, typesize, block_size, use_dict, content, running_time, running_counts, meta)
        #     # x(data_stream, blosc_comp_level, typesize, 0, use_dict, content, running_time, running_counts, meta)
        
        # done forever
        # y(data_stream, comp_level, content, running_time, running_counts)
        # w(data_stream, comp_level, content, running_time, running_counts)
        print_stuff(data_stream, running_counts, running_time)


""" pyzstd has the best speed, good number of extra strategies that are worth exploing 
- fast falls apart starting on compression level 5, is very slight worse than dfast in ratio, very slightly better in speed
- dfast has an standout performance/compression ratio at compression level 1

fast
falls apart starting at 4 or 5, also dfast is always better
18% high
accelerometer_pyzstd_00_strat1fast	18.87%	1.64(sec)	559.29	MB/s encode	1688.62	MB/s decode  # borked
accelerometer_pyzstd_01_strat1fast	18.83%	1.52(sec)	603.08	MB/s encode	1951.83	MB/s decode
accelerometer_pyzstd_02_strat1fast	18.71%	1.6(sec)	570.75	MB/s encode	1868.95	MB/s decode
accelerometer_pyzstd_03_strat1fast	18.87%	1.61(sec)	569.13	MB/s encode	1744.81	MB/s decode
accelerometer_pyzstd_04_strat1fast	19.12%	1.67(sec)	547.42	MB/s encode	1752.9	MB/s decode # borked
accelerometer_pyzstd_05_strat1fast	23.81%	1.62(sec)	563.47	MB/s encode	1849.71	MB/s decode # borked

dfast
18% mid
accelerometer_pyzstd_00_strat2dfast	18.62%	1.91(sec)	478.33	MB/s encode	1766.36	MB/s decode  # weirdly always slightly slower
accelerometer_pyzstd_01_strat2dfast	18.68%	1.6(sec)	572.55	MB/s encode	2052.85	MB/s decode  # actually strictly better than fast....
accelerometer_pyzstd_02_strat2dfast	18.59%	1.78(sec)	514.16	MB/s encode	1922.05	MB/s decode  

greedy
stops improving after 5, half the speed of dfast/fast
18% low
18.24 / 18.71 = 0.974879743452699   @ 260.01 / 570.75 = 0.454978354978355
accelerometer_pyzstd_00_strat3greedy	18.31%	3.47(sec)	263.64	MB/s encode	1772.59	MB/s decode 
accelerometer_pyzstd_01_strat3greedy	18.59%	3.56(sec)	257.41	MB/s encode	1988.16	MB/s decode
accelerometer_pyzstd_02_strat3greedy	18.34%	3.4(sec)	268.99	MB/s encode	1939.89	MB/s decode
accelerometer_pyzstd_03_strat3greedy	18.31%	3.43(sec)	266.82	MB/s encode	1791.39	MB/s decode
accelerometer_pyzstd_04_strat3greedy	18.24%	3.52(sec)	260.01	MB/s encode	1787.86	MB/s decode # first strictly better compression, best in class generally
accelerometer_pyzstd_05_strat3greedy	18.14%	4.05(sec)	226.14	MB/s encode	1803.36	MB/s decode # and again, but slower

lazy 1/2-1/3rd the speed of dfast
always better compression, always slower than greedy
ALL 17%
17.85 / 18.71 = 0.954035275253875  @ 207.67 / 570.75 = 0.36385457731055626
17.84 / 18.71 = 0.9535008017103153 @ 203.89 / 570.75 = 0.3572317126587823
accelerometer_pyzstd_00_strat4lazy	17.9%	4.44(sec)	205.95	MB/s encode	1841.09	MB/s decode # weird
accelerometer_pyzstd_01_strat4lazy	17.99%	4.64(sec)	197.43	MB/s encode	2019.93	MB/s decode 
accelerometer_pyzstd_02_strat4lazy	17.85%	4.41(sec)	207.67	MB/s encode	1984.99	MB/s decode # REALLY GOOD ACTUALLY!!!!!!
accelerometer_pyzstd_03_strat4lazy	17.9%	4.42(sec)	207.24	MB/s encode	1857.36	MB/s decode # same as 0?
accelerometer_pyzstd_04_strat4lazy	17.84%	4.49(sec)	203.89	MB/s encode	1863.41	MB/s decode # similarly good!!!!!!
accelerometer_pyzstd_05_strat4lazy	17.72%	5.6(sec)	163.44	MB/s encode	1879.64	MB/s decode # always worse than lazy2 in compression, better speed
accelerometer_pyzstd_06_strat4lazy	17.73%	5.48(sec)	167.05	MB/s encode	1891.92	MB/s decode # strictly better than 5
accelerometer_pyzstd_07_strat4lazy	17.68%	6.53(sec)	140.1	MB/s encode	1878.77	MB/s decode 
accelerometer_pyzstd_08_strat4lazy	17.67%	6.44(sec)	142.02	MB/s encode	1887.12	MB/s decode

lazy2
1/3rd -> 1/4th the speed of dfast, 
ALL 17%
17.39 / 18.71 = 0.9294494922501336 @ 172.45 / 570.75 = 0.3021462987297415
17.33 / 18.71 = 0.9262426509887759 @ 169.78 / 570.75 = 0.2974682435392028
17.1 / 18.71 = 0.9139497594869054 @ 102.16 / 570.75 = 0.17899255365746825
accelerometer_pyzstd_00_strat5lazy2	17.39%	5.37(sec)	170.4	MB/s encode	1933.88	MB/s decode # weird
accelerometer_pyzstd_01_strat5lazy2	17.61%	5.48(sec)	166.87	MB/s encode	2031.01	MB/s decode # better than any compression level
accelerometer_pyzstd_02_strat5lazy2	17.4%	5.26(sec)	174.11	MB/s encode	2039.82	MB/s decode
accelerometer_pyzstd_03_strat5lazy2	17.39%	5.31(sec)	172.45	MB/s encode	1960.97	MB/s decode # same as 0?
accelerometer_pyzstd_04_strat5lazy2	17.33%	5.39(sec)	169.78	MB/s encode	1964.72	MB/s decode
accelerometer_pyzstd_05_strat5lazy2	17.19%	7.1(sec)	128.86	MB/s encode	1987.65	MB/s decode # performance drop 1
accelerometer_pyzstd_06_strat5lazy2	17.19%	6.97(sec)	131.27	MB/s encode	1992.99	MB/s decode
accelerometer_pyzstd_07_strat5lazy2	17.11%	8.61(sec)	106.24	MB/s encode	1987.65	MB/s decode # performance drop 2
accelerometer_pyzstd_08_strat5lazy2	17.11%	8.53(sec)	107.33	MB/s encode	2000.82	MB/s decode
accelerometer_pyzstd_09_strat5lazy2	17.1%	8.96(sec)	102.16	MB/s encode	1981.33	MB/s decode

btlazy2
1/4th ->1/12th the speed of dfast,
accelerometer_pyzstd_00_strat6btlazy2	17.36%	8.0(sec)	114.44	MB/s encode	1895.01	MB/s decode
accelerometer_pyzstd_01_strat6btlazy2	17.53%	7.94(sec)	115.33	MB/s encode	1966.9	MB/s decode
accelerometer_pyzstd_02_strat6btlazy2	17.34%	7.73(sec)	118.32	MB/s encode	1977.8	MB/s decode  # this random one is as good compression as lazy2 @ 4, but always slower than lazy2
accelerometer_pyzstd_03_strat6btlazy2	17.36%	7.85(sec)	116.56	MB/s encode	1936.66	MB/s decode
accelerometer_pyzstd_04_strat6btlazy2	17.3%	8.7(sec)	105.24	MB/s encode	1940.12	MB/s decode
accelerometer_pyzstd_05_strat6btlazy2	16.88%	15.61(sec)	58.62	MB/s encode	1939.71	MB/s decode  # new best compression ratio
accelerometer_pyzstd_06_strat6btlazy2	16.88%	15.49(sec)	59.09	MB/s encode	1947.92	MB/s decode
accelerometer_pyzstd_07_strat6btlazy2	16.6%	21.53(sec)	42.52	MB/s encode	2007.04	MB/s decode
accelerometer_pyzstd_08_strat6btlazy2	16.6%	21.37(sec)	42.83	MB/s encode	2021.2	MB/s decode
accelerometer_pyzstd_09_strat6btlazy2	16.57%	23.46(sec)	39.0	MB/s encode	2002.7	MB/s decode

VERY SLOW AND BAD
btopt
accelerometer_pyzstd_00_strat7btopt	18.52%	18.25(sec)	50.14	MB/s encode	1615.08	MB/s decode
accelerometer_pyzstd_01_strat7btopt	18.94%	18.43(sec)	49.66	MB/s encode	1777.3	MB/s decode
accelerometer_pyzstd_02_strat7btopt	18.44%	18.42(sec)	49.67	MB/s encode	1759.04	MB/s decode
accelerometer_pyzstd_03_strat7btopt	18.52%	18.0(sec)	50.85	MB/s encode	1650.81	MB/s decode
accelerometer_pyzstd_04_strat7btopt	18.37%	18.93(sec)	48.35	MB/s encode	1660.89	MB/s decode
accelerometer_pyzstd_05_strat7btopt	18.02%	35.2(sec)	26.0	MB/s encode	1679.88	MB/s decode
accelerometer_pyzstd_06_strat7btopt	18.05%	35.15(sec)	26.04	MB/s encode	1741.22	MB/s decode
accelerometer_pyzstd_07_strat7btopt	16.93%	45.91(sec)	19.93	MB/s encode	1834.1	MB/s decode
accelerometer_pyzstd_08_strat7btopt	17.11%	49.33(sec)	18.55	MB/s encode	1902.11	MB/s decode
accelerometer_pyzstd_09_strat7btopt	17.07%	52.41(sec)	17.46	MB/s encode	1895.66	MB/s decode




gyro_pyzstd_00_strat2dfast	14.16%	0.27(sec)	578.3	MB/s encode	1781.06	MB/s decode
gyro_pyzstd_01_strat2dfast	14.1%	0.23(sec)	674.3	MB/s encode	2001.48	MB/s decode
gyro_pyzstd_02_strat2dfast	13.97%	0.25(sec)	631.98	MB/s encode	1888.99	MB/s decode  # nice
gyro_pyzstd_01_strat4lazy	13.13%	0.68(sec)	231.89	MB/s encode	2050.26	MB/s decode
gyro_pyzstd_02_strat4lazy	13.24%	0.67(sec)	237.18	MB/s encode	2002.93	MB/s decode
gyro_pyzstd_04_strat4lazy	13.35%	0.7(sec)	226.36	MB/s encode	1812.45	MB/s decode
gyro_pyzstd_05_strat4lazy	12.83%	0.84(sec)	187.26	MB/s encode	1985.83	MB/s decode
gyro_pyzstd_06_strat4lazy	12.83%	0.82(sec)	191.95	MB/s encode	1995.08	MB/s decode
gyro_pyzstd_07_strat4lazy	12.7%	0.98(sec)	161.72	MB/s encode	2051.06	MB/s decode
gyro_pyzstd_08_strat4lazy	12.69%	0.94(sec)	167.95	MB/s encode	2093.56	MB/s decode
gyro_pyzstd_09_strat4lazy	12.69%	1.05(sec)	149.98	MB/s encode	2065.34	MB/s decode
"""
def compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, strat):
    key = data_stream + "_pyzstd_" + comp_disp(comp_level) + "_strat" + str(int(strat)) + PYZSTD_STRAT_NAMES[strat]
    t1 = perf_counter()
    thang = pyzstd.RichMemZstdCompressor(
        { # type: ignore
            pyzstd.CParameter.compressionLevel: comp_level,
            pyzstd.CParameter.nbWorkers: 1,
            pyzstd.CParameter.enableLongDistanceMatching: 1,
            pyzstd.CParameter.strategy: strat
        },
        # the_zstd_dictionary,
    )
    # output = pyzstd.richmem_compress(
    #     content,
    #     comp_level,
    #     # the_zstd_dictionary,
    # )
    output = thang.compress(content)
    t1 = perf_counter() - t1
    
    # the zstd dictionary miiiight be useful if we can make it good, but it cuts speed in half,
    # and basic tests show it doesn't improve compression at all woo!
    
    try:
        t2 = perf_counter()
        decompressed_output = pyzstd.decompress(
            output,
            # the_zstd_dictionary
        )
        t2 = perf_counter() - t2
    except zstd.Error:
        print()
        print(f"'{content}'")
        exit()
    
    assert decompressed_output == content
    running_time[key] += t1
    running_counts[key] += len(output)
    running_time[key + "_dec"] += t2
    running_counts[key + "_dec"] += len(decompressed_output)


""" even faster, but possibly problematic
- need to determine whether this actually creates zstd standard binaries that can be decompressed by other libraries.  it might not?
- there are 3rd party additional compression options that _I cannot get working_
- has multiple 
Blosc may be better once we get to binary compression formats
blosc allows 9 compression levels many other options
accelerometer_blosc_zstd_01_NOFIL_ts8_blk134217792_dTrue_m00	18.84%	1.57(sec)	583.36	MB/s
accelerometer_blosc_zstd_01_NOFIL_ts8_blk134217792_dTrue_m00_dec 1687.69 MB/s
accelerometer_blosc_zstd_02_NOFIL_ts8_blk134217792_dTrue_m00	18.56%	1.94(sec)	472.92	MB/s
accelerometer_blosc_zstd_02_NOFIL_ts8_blk134217792_dTrue_m00_dec 1767.43 MB/s
accelerometer_blosc_zstd_03_NOFIL_ts8_blk134217792_dTrue_m00	18.14%	4.4(sec)	208.23	MB/s
accelerometer_blosc_zstd_03_NOFIL_ts8_blk134217792_dTrue_m00_dec 1766.57 MB/s
accelerometer_blosc_zstd_04_NOFIL_ts8_blk134217792_dTrue_m00	17.67%	7.02(sec)	130.41	MB/s
accelerometer_blosc_zstd_04_NOFIL_ts8_blk134217792_dTrue_m00_dec 1850.65 MB/s
accelerometer_blosc_zstd_05_NOFIL_ts8_blk134217792_dTrue_m00	17.11%	9.57(sec)	95.6	MB/s
accelerometer_blosc_zstd_05_NOFIL_ts8_blk134217792_dTrue_m00_dec 1966.04 MB/s
accelerometer_blosc_zstd_06_NOFIL_ts8_blk134217792_dTrue_m00	16.8%	17.39(sec)	52.64	MB/s
accelerometer_blosc_zstd_06_NOFIL_ts8_blk134217792_dTrue_m00_dec 2011.77 MB/s
accelerometer_blosc_zstd_07_NOFIL_ts8_blk134217792_dTrue_m00	16.54%	30.19(sec)	30.31	MB/s
accelerometer_blosc_zstd_07_NOFIL_ts8_blk134217792_dTrue_m00_dec 1999.54 MB/s
accelerometer_blosc_zstd_08_NOFIL_ts8_blk134217792_dTrue_m00	16.29%	38.21(sec)	23.95	MB/s
accelerometer_blosc_zstd_08_NOFIL_ts8_blk134217792_dTrue_m00_dec 2032.76 MB/s
accelerometer_blosc_zstd_09_NOFIL_ts8_blk134217792_dTrue_m00	14.42%	180.55(sec)	5.07	MB/s
accelerometer_blosc_zstd_09_NOFIL_ts8_blk134217792_dTrue_m00_dec 2031.48 MB/s
"""
def compress_blosc(
    data_stream, comp_level, typesize, block_size, use_dict, content, running_time,
    running_counts, meta
):
    for a_filter in BLOSC_FILTERS:
        key = data_stream + "_blosc_zstd_" + comp_disp(comp_level) \
                + "_" + a_filter.name[:5] + "_ts" + str(typesize) + "_blk" + str(block_size) \
                + "_d" + str(use_dict) + "_m" + comp_disp(meta)
        # print(key, comp_level, a_filter, typesize, block_size, use_dict, meta)
        t1 = perf_counter()
        output = blosc2.compress2(
            content,
            clevel=comp_level,
            filters={0, 0, 0, 0, 0, 0},  # any value not 0 (no filter) kill compression ratio
            # filters={a_filter.value, a_filter.value, a_filter.value, a_filter.value, a_filter.value, a_filter.value},  # any value not 0 (no filter) kill compression ratio
            # filters={0, a_filter.value, 0, 0, 0, 0},  # no effect?
            filters_meta={0, 0, 0, 0, 0, 0},  # no effect?
            codec=blosc2.Codec.ZSTD,
            # codec=blosc2.Codec.LZ4HC,      # not as good compression, ~30%, 700 MB/s / 4000 MB/s
            # codec=blosc2.Codec.LZ4,        # ignores compression level
            # codec=blosc2.Codec.ZLIB,
            # codec=blosc2.Codec.BLOSCLZ,    # VERY FAST. VERY NOT GOOD COMPRESSION
            # codec=blosc2.Codec.NDLZ,       # crash
            # codec=blosc2.Codec.ZFP_ACC,    # no compression
            # codec=blosc2.Codec.ZFP_PREC,   # no compression
            # codec=blosc2.Codec.ZFP_RATE,   # no compression
            # codec=blosc2.Codec.OPENHTJ2K,  # no compression
            # codec_meta=200,  # no apparent effect
            typesize=typesize,
            nthreads=1,  # no effect when block size is non-zero and block size is large
            blocksize=block_size,
            use_dict=False,
            # splitmode=blosc2.NEVER_SPLIT,
        )
        # blosc2.free_resources()
        # gc.collect(2)
        t1 = perf_counter() - t1
        
        t2 = perf_counter()
        decompressed_output1 = blosc2.decompress(output)
        t2 = perf_counter() - t2
        assert decompressed_output1 == content
        
        # t3 = perf_counter()
        # decompressed_output2 = bytearray(len(content))
        # blosc2.decompress(output, decompressed_output2)
        # t3 = perf_counter() - t3
        # assert decompressed_output2 == content
        
        running_time[key] += t1
        running_counts[key] += len(output)
        running_time[key+"_dec"] += t2
        running_counts[key + "_dec"] += len(decompressed_output1)


""" Always the slowest """
def compress_zstd(data_stream, comp_level, content, running_time, running_counts):
    key = data_stream + "_zstd_" + comp_disp(comp_level)
    t1 = perf_counter()
    output = zstd.compress(
        content,
        comp_level,
        1,  # auto-tune the number of threads (no real drawbacks)
    )
    t1 = perf_counter() - t1
    
    try:
        t2 = perf_counter()
        decompressed_output = zstd.decompress(output)
        t2 = perf_counter() - t2
    except zstd.Error:
        print()
        print(f"'{content}'")
        exit()
    
    assert decompressed_output == content
    running_time[key] += t1
    running_counts[key] += len(output)
    running_time[key + "_dec"] += t2
    running_counts[key + "_dec"] += len(decompressed_output)


""" not quite as good as pyzstd """
def compress_zstandard(data_stream, comp_level, content, running_time, running_counts):
    # ZstdCompressor(level=1, threads=1).compress
    # ZstdDecompressor().decompress
    key = data_stream + "_zstandard_" + comp_disp(comp_level)
    t1 = perf_counter()
    output = ZstdCompressor(level=comp_level, threads=1).compress(content)
    
    t1 = perf_counter() - t1
    
    try:
        t2 = perf_counter()
        decompressed_output = ZstdDecompressor().decompress(output)
        t2 = perf_counter() - t2
    except None:
        print()
        print(f"'{content}'")
        exit()
    
    assert decompressed_output == content
    running_time[key] += t1
    running_counts[key] += len(output)
    running_time[key + "_dec"] += t2
    running_counts[key + "_dec"] += len(decompressed_output)


def make_the_dict():
    print("reading data")
    z = [content for participant_id, data_stream, content in iterate_all_files()]
    print("training")
    x = pyzstd.train_dict(
        z,
        1_000,
    )
    x.as_digested_dict
    with open("the_zstd_dictionary", "wb") as f:
        f.write(x.dict_content)

# make_the_dict()
# exit()

with open("the_zstd_dictionary", "rb") as f:
    loaded_dict_content = f.read()
    the_zstd_dictionary = pyzstd.ZstdDict(loaded_dict_content)

# main_pyzstd()
# main_zstd()
# main_blosc()
# main_zstandard()
main_all()



# Some reference numbers:
# pyzstd at level 22 with btultra2
# accelerometer_pyzstd_22 14.427897359764339% 259.4491169482062sec 3.5275185125916426 MB/s
# accelerometer_pyzstd_22_dec 1350.9554818136533 MB/s
# accelerometer_pyzstd_22_ttl 260.12657196092186sec 3.5183316960323507 MB/s

# accelerometer_pyzstd_-01_strat1fast 24.84211804388197% 2.0619864693435375sec 443.8494513505329 MB/s
# accelerometer_pyzstd_-01_strat1fast_ttl 2.7881568941811565sec 328.2496637905797 MB/s
# accelerometer_pyzstd_-01_strat2dfast 18.814232154740708% 2.348831542331027sec 389.64546695506203 MB/s
# accelerometer_pyzstd_-01_strat2dfast_ttl 3.0595005934883375sec 299.1375667840152 MB/s
# accelerometer_pyzstd_00_strat1fast 18.865636343235387% 2.5856467652483843sec 353.9584661799052 MB/s
# accelerometer_pyzstd_00_strat1fast_ttl 3.4451769108709414sec 265.6500919364939 MB/s
# accelerometer_pyzstd_00_strat2dfast 18.62185559252479% 2.9708075411908794sec 308.06827787419695 MB/s
# accelerometer_pyzstd_00_strat2dfast_ttl 3.752012076729443sec 243.92553765661756 MB/s
# accelerometer_pyzstd_01_strat1fast 18.82705074275202% 2.1878779174003284sec 418.31016065001495 MB/s
# accelerometer_pyzstd_01_strat1fast_ttl 2.9564087889739312sec 309.5686789065427 MB/s
# accelerometer_pyzstd_01_strat2dfast 18.684431342610587% 2.4298923571768682sec 376.6469573877238 MB/s
# accelerometer_pyzstd_01_strat2dfast_ttl 3.1450094035826623sec 291.0043963836105 MB/s
# accelerometer_pyzstd_02_strat1fast 18.707921944910833% 2.5376026179874316sec 360.6599223310245 MB/s
# accelerometer_pyzstd_02_strat1fast_ttl 3.3661482431925833sec 271.8868858379006 MB/s
# accelerometer_pyzstd_02_strat2dfast 18.586480161782468% 2.8599350860749837sec 320.0113063987061 MB/s
# accelerometer_pyzstd_02_strat2dfast_ttl 3.619211460056249sec 252.87595743193395 MB/s

# it keeps coming back to the default level 1
# accelerometer_pyzstd_-01_strat2dfast 18.814232154740708% 2.3824365963519085sec 384.14938912194503 MB/s
# accelerometer_pyzstd_-01_strat2dfast_ttl 3.1382934563735034sec 291.6271457188508 MB/s
# accelerometer_pyzstd_00_strat2dfast 18.62185559252479% 2.8882915131398477sec 316.8695261356876 MB/s
# accelerometer_pyzstd_00_strat2dfast_ttl 3.695901872444665sec 247.6287506261583 MB/s
# accelerometer_pyzstd_01_strat2dfast 18.684431342610587% 2.3960867217101622sec 381.9609510865852 MB/s
# accelerometer_pyzstd_01_strat2dfast_ttl 3.0722432311740704sec 297.89684417681985 MB/s
# accelerometer_pyzstd_02_strat2dfast 18.586480161782468% 2.831741788861109sec 323.197392753256 MB/s
# accelerometer_pyzstd_02_strat2dfast_ttl 3.5656242285040207sec 256.6763922552585 MB/s
# accelerometer_pyzstd_03_strat2dfast 18.62185559252479% 2.847597050393233sec 321.3978476989809 MB/s
# accelerometer_pyzstd_03_strat2dfast_ttl 3.610620494815521sec 253.47764031819491 MB/s

# this is as above but with long matching enabled - no advantage, its just slower wit no compression advantage
# accelerometer_pyzstd_-01_strat2dfast 18.814884150458226% 2.915807848155964sec 313.8792440280851 MB/s
# accelerometer_pyzstd_-01_strat2dfast_ttl 3.640427972219186sec 251.40218954873137 MB/s
# accelerometer_pyzstd_00_strat2dfast 18.62172950736925% 3.4572509804274887sec 264.72233815005984 MB/s
# accelerometer_pyzstd_00_strat2dfast_ttl 4.24033301891177sec 215.83483161075625 MB/s
# accelerometer_pyzstd_01_strat2dfast 18.68911931373663% 2.994416996574728sec 305.63931615311077 MB/s
# accelerometer_pyzstd_01_strat2dfast_ttl 3.664363143587252sec 249.76006122973916 MB/s
# accelerometer_pyzstd_02_strat2dfast 18.586316980498523% 3.4380317706090864sec 266.20218316022465 MB/s
# accelerometer_pyzstd_02_strat2dfast_ttl 4.155296734912554sec 220.2517946361806 MB/s
# accelerometer_pyzstd_03_strat2dfast 18.62172950736925% 3.4424614355375525sec 265.8596414944117 MB/s
# accelerometer_pyzstd_03_strat2dfast_ttl 4.232667425880209sec 216.22572033757834 MB/s

# lazy2 does get a 1% compression ratio improvement, at the cost of 3-4x improvement
# with long distance match enabled
# accelerometer_pyzstd_-01_strat5lazy2 17.9026510685644% 8.797023562307004sec 104.03650241790861 MB/s
# accelerometer_pyzstd_-01_strat5lazy2_ttl 9.510497681010747sec 96.23172138906253 MB/s
# accelerometer_pyzstd_00_strat5lazy2 17.3857197495036% 8.756556238629855sec 104.51729403311128 MB/s
# accelerometer_pyzstd_00_strat5lazy2_ttl 9.505714073660783sec 96.28014855257379 MB/s
# accelerometer_pyzstd_01_strat5lazy2 17.596144307607435% 8.507382576935925sec 107.57851252529191 MB/s
# accelerometer_pyzstd_01_strat5lazy2_ttl 9.189714668056695sec 99.5908574062275 MB/s
# accelerometer_pyzstd_02_strat5lazy2 17.39893086873881% 8.488137664593523sec 107.82242221729787 MB/s
# accelerometer_pyzstd_02_strat5lazy2_ttl 9.171466214349493sec 99.7890131981765 MB/s
# accelerometer_pyzstd_03_strat5lazy2 17.3857197495036% 8.585160190792521sec 106.60390054129738 MB/s
# accelerometer_pyzstd_03_strat5lazy2_ttl 9.296137910714606sec 98.45072995910385 MB/s
# # with long distance match disabled (virtually no difference):
# accelerometer_pyzstd_-01_strat5lazy2 17.90271692461258% 8.346282955404604sec 109.65498869382445 MB/s
# accelerometer_pyzstd_-01_strat5lazy2_ttl 9.065662020933814sec 100.95363813442492 MB/s
# accelerometer_pyzstd_00_strat5lazy2 17.38572964875135% 8.1247005990881sec 112.64557406743985 MB/s
# accelerometer_pyzstd_00_strat5lazy2_ttl 8.869396195106674sec 103.18758379688597 MB/s
# accelerometer_pyzstd_01_strat5lazy2 17.59622120913205% 8.052082658949075sec 113.66147143226188 MB/s
# accelerometer_pyzstd_01_strat5lazy2_ttl 8.732740917796036sec 104.8023262942893 MB/s
# accelerometer_pyzstd_02_strat5lazy2 17.39898453308187% 7.924178491230123sec 115.49608128126316 MB/s
# accelerometer_pyzstd_02_strat5lazy2_ttl 8.616610942524858sec 106.21479479752098 MB/s
# accelerometer_pyzstd_03_strat5lazy2 17.38572964875135% 8.135862692870433sec 112.4910286296208 MB/s
# accelerometer_pyzstd_03_strat5lazy2_ttl 8.84643590435735sec 103.4553997796514 MB/s
# accelerometer_pyzstd_04_strat5lazy2 17.33267197327478% 8.258503748016665sec 110.82050587313056 MB/s
# accelerometer_pyzstd_04_strat5lazy2_ttl 8.987103706720518sec 101.8360968090265 MB/s
# accelerometer_pyzstd_05_strat5lazy2 17.187259734836875% 12.172071184089873sec 75.18946851926282 MB/s
# accelerometer_pyzstd_05_strat5lazy2_ttl 12.889921113091987sec 71.00210738922156 MB/s
# accelerometer_pyzstd_06_strat5lazy2 17.194203796626187% 11.782758350775111sec 77.67379554636676 MB/s
# accelerometer_pyzstd_06_strat5lazy2_ttl 12.471733416954521sec 73.38286768270561 MB/s

# and then a comparison at level 4 - fast is weirdly bad, dfast is fine?
# accelerometer_pyzstd_04_strat1fast 19.11862433217591% 2.5209913223225158sec 363.0363797790442 MB/s
# accelerometer_pyzstd_04_strat1fast_ttl 3.335239332402125sec 274.40656333684836 MB/s
# accelerometer_pyzstd_04_strat2dfast 18.57543760301991% 2.898036094091367sec 315.80405950647815 MB/s
# accelerometer_pyzstd_04_strat2dfast_ttl 3.6965345921344124sec 247.5863650938811 MB/s
# accelerometer_pyzstd_04_strat3greedy 18.233474029083865% 5.33593176244176sec 171.51860328355178 MB/s
# accelerometer_pyzstd_04_strat3greedy_ttl 6.116708958608797sec 149.6248340902769 MB/s
# accelerometer_pyzstd_04_strat4lazy 17.837352400132012% 7.001984578528209sec 130.70745198680882 MB/s
# accelerometer_pyzstd_04_strat4lazy_ttl 7.824774221924599sec 116.96331896017877 MB/s
# accelerometer_pyzstd_04_strat5lazy2 17.33267197327478% 8.508921316708438sec 107.55905819850604 MB/s
# accelerometer_pyzstd_04_strat5lazy2_ttl 9.250235266052186sec 98.9392741684229 MB/s


# trying dfast higher compression levvels (junk, and it actually gets Worse compression above that)
# accelerometer_pyzstd_04_strat2dfast 18.57543760301991% 2.9216221861133818sec 313.25459104890376 MB/s
# accelerometer_pyzstd_04_strat2dfast_ttl 3.7024032537592575sec 247.19391713506246 MB/s
# accelerometer_pyzstd_05_strat2dfast 18.54260336127603% 2.9087526479561348sec 314.6405603630252 MB/s
# accelerometer_pyzstd_05_strat2dfast_ttl 3.7201913144090213sec 246.01196168744332 MB/s
# accelerometer_pyzstd_06_strat2dfast 18.542349523723225% 2.896736924041761sec 315.9456958325972 MB/s
# accelerometer_pyzstd_06_strat2dfast_ttl 3.6880353896995075sec 248.1569362556797 MB/s
# accelerometer_pyzstd_07_strat2dfast 18.560533399811817% 3.351718399382662sec 273.0574153481749 MB/s
# accelerometer_pyzstd_07_strat2dfast_ttl 4.154217225179309sec 220.30902899422844 MB/s
# accelerometer_pyzstd_08_strat2dfast 18.560533399811817% 3.2415586122660898sec 282.33688561026844 MB/s
# accelerometer_pyzstd_08_strat2dfast_ttl 4.026910374261206sec 227.27388445496757 MB/s
# accelerometer_pyzstd_08_strat2dfast 18.560533399811817% 3.46947242951137sec 263.7898359778138 MB/s
# accelerometer_pyzstd_08_strat2dfast_ttl 4.253350788727403sec 215.17424933206172 MB/s
# accelerometer_pyzstd_09_strat2dfast 18.55432052772202% 4.227544096735073sec 216.4877626745912 MB/s
# accelerometer_pyzstd_09_strat2dfast_ttl 5.001575757021783sec 182.9846447543003 MB/s
# accelerometer_pyzstd_10_strat2dfast 18.551627932334327% 4.736803104489809sec 193.21292080788885 MB/s
# accelerometer_pyzstd_10_strat2dfast_ttl 5.525300215376774sec 165.64015120180085 MB/s
# accelerometer_pyzstd_11_strat2dfast 18.551766417600206% 4.586710803792812sec 199.53548463390172 MB/s
# accelerometer_pyzstd_11_strat2dfast_ttl 5.370270015031565sec 170.42188950437202 MB/s
# accelerometer_pyzstd_12_strat2dfast 18.55133991632614% 4.856838401028654sec 188.4377217320046 MB/s
# accelerometer_pyzstd_12_strat2dfast_ttl 5.639553244691342sec 162.2844086048596 MB/s
