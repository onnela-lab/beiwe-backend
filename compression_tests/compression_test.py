import os
from time import perf_counter
from typing import DefaultDict

import blosc2
import pyzstd
from blosc2 import Filter


# AFTER A LOT OF TESTING IT HAS BECOME CLEAR THAT PYZSTD AND BLOSC ARE THE FASTEST ZSTD IMPLEMENTATIONS
# I AM REMOVING REFERENCES TO THE ZSTANDARD LIBRARY AND THE ZSTD LIBRARY.  THEY ARE ALWAYS SLOWER.
# OTHER COMPRESSION OPTIONS WERE TESTED AND DISCARDED, ZSTD IS THE BEST.

# ALL MEASUREMENTS LISTED IN THIS DOCUMENT CANNOT BE COMPARED _DIRECTLY_ TO OTHER COMPUTERS.
# Compression ratios should be constant even if speeds are not, assume differences between options
# are roughly constant.

blosc2.set_releasegil(False)  # ok that would be nice - need to test if it blocks
blosc2.set_nthreads(1)

# these are to makey the printing nice
BLOSC_FILTER_NAMES_MAP = {       # these
    0: "NOFILTER  ",       32: "NDCELL",
    1: "SHUFFLE   ",       33: "NDMEAN",
    2: "BITSHUFFLE",       35: "BYTEDELTA",
    3: "DELTA     ",       36: "INT_TRUNC",
    4: "TRUNC_PREC",
}
BLOSC_CODECS_NAMES_MAP = {                   # these are ~new - but crashed?
    blosc2.Codec.BLOSCLZ:  "BLOSCLZ  ",      blosc2.Codec.NDLZ:     "NDLZ     ",
    blosc2.Codec.LZ4:      "LZ4      ",      blosc2.Codec.ZFP_ACC:  "ZFP_ACC  ",
    blosc2.Codec.ZSTD:     "ZSTD     ",      blosc2.Codec.ZFP_PREC: "ZFP_PREC ",
    blosc2.Codec.ZLIB:     "ZLIB     ",      blosc2.Codec.ZFP_RATE: "ZFP_RATE ",
    blosc2.Codec.LZ4HC:    "LZ4HC    ",
}
PYZSTD_STRAT_NAMES = {
    pyzstd.Strategy.fast:   "fast     ",       pyzstd.Strategy.btlazy2:  "btlazy2  ",
    pyzstd.Strategy.dfast:  "dfast    ",       pyzstd.Strategy.btopt:    "btopt    ",
    pyzstd.Strategy.greedy: "greedy   ",       pyzstd.Strategy.btultra:  "btultra  ",
    pyzstd.Strategy.lazy:   "lazy     ",       pyzstd.Strategy.btultra2: "btultra2 ",
    pyzstd.Strategy.lazy2:  "lazy2    ",
}

BLOSC_FILTERS = [
    Filter.NOFILTER,  # All the other ones get half compression LEVEL and THROUGHPUT on RAW CSV DATA
    # Filter.SHUFFLE,       Filter.NDCELL,
    # Filter.BITSHUFFLE,    Filter.NDMEAN,
    # Filter.DELTA,         Filter.BYTEDELTA,
    # Filter.TRUNC_PREC,    Filter.INT_TRUNC,  # these two are lossy - should be investigated.
]

PYZSTD_STRATEGIES = [          # ordered from fastest to slowest, and ~ worst to best compression,
    # pyzstd.Strategy.fast,    # there are extensive notes and examples later in the file.
    pyzstd.Strategy.dfast,     # Some strategies only work well at certain ranges of compression levels.
    # pyzstd.Strategy.greedy,
    # pyzstd.Strategy.lazy,
    # pyzstd.Strategy.lazy2,
    # pyzstd.Strategy.btlazy2,
    # pyzstd.Strategy.btopt,
    # pyzstd.Strategy.btultra,
    # pyzstd.Strategy.btultra2,
]

from os.path import join as path_join


# this is a folder with user's data in it directly as downloaded from Beiwe
DATA_FOLDER = "./private/data"  # . is the location python was run from, not the location of the script.

def iterate_all_files():
    # for every participant folder
    for user_path in os.listdir(DATA_FOLDER):
        # for every datastream in the participant folder
        for datastream in os.listdir(path_join(DATA_FOLDER, user_path)):
            # Pick one data stream, compression is only consistent for a single data stream
            # if datastream != "accelerometer":
            if datastream != "gyro":
            # if datastream != "gps":
            # if datastream != "wifi":
            # if datastream != "texts":
            # if datastream != "ios_log":
            # if datastream != "app_log":
            # low quantities of data:
            # if datastream != "reachability":
            # if datastream != "power_state":
            # if datastream != "identifiers":
            # if datastream != "calls":
                continue
            
            # go through datastream files and read them in
            datastream_path = path_join(DATA_FOLDER, f"{user_path}/{datastream}/")
            for datastream_file in os.listdir(datastream_path):
                datastream_file_path = f"{datastream_path}/{datastream_file}"
                if not os.path.isdir(datastream_file_path):
                    with open(datastream_file_path, "rb") as f:
                        data: bytes = f.read()
                        if data:
                            yield user_path, datastream, data


def print_stats(data_stream, running_counts, compress_time):
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
    # normalizes length of some strings?
    if i < 0:
        return "-0" + str(abs(i))
    elif i < 10:
        return "0" + str(i)
    else:
        return "" + str(i)


# might not work anymore....
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
            print_stats(data_stream, running_counts, running_time)


# might not work anymore....
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
        print_stats(data_stream, running_counts, running_time)


def run_both():
    running_counts = DefaultDict(int)
    running_time = DefaultDict(float)
    meta = 0  # 126  # is randomly good?
    use_dict = False
    typesize = 8
    block_size = 1024*128*1024 + 64  # this value happens to be good, unclear why.
    
    for participant_id, data_stream, content in iterate_all_files():
        running_counts[data_stream] += len(content)
        
        for comp_level in [0,1,2,3,4,5,6,7,8,9]:  # goes up to 22
            # if comp_level in [0,1,2,3,4,5]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.fast)
            if comp_level in [0,1,2]:
                compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.dfast)
            # if comp_level in [0,1,2,3,4,5]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.greedy)
            # if comp_level in [1,3,5,7,9]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.lazy)
            # if comp_level in [1,2,4,5,6,7,8,9]:
            # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.lazy2)
            # if comp_level in [1,2,4,5,6,7,8,9]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btlazy2)
            # SLOOOWWWW
            # if comp_level in [1,2,4,5,6,7,8,9]:
            #     compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btopt)
            # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btopt)
            # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btultra)
            # compress_pyzstd(data_stream, comp_level, content, running_time, running_counts, pyzstd.Strategy.btultra2)
        
        # for blosc_comp_level in [1,2,3]:  # goes up to 9.
        #     # there are only two good block sizes.
        #     compress_blosc(data_stream, blosc_comp_level, typesize, block_size, use_dict, content, running_time, running_counts, meta)
        #     # compress_blosc(data_stream, blosc_comp_level, typesize, 0, use_dict, content, running_time, running_counts, meta)
        
        print_stats(data_stream, running_counts, running_time)


""" pyzstd has the best speed, good number of extra strategies that are worth exploing 
pyzstd uses has 0-22 compression levels (this is normal) in increasing compression and decreasing speed. 

- fast falls apart starting on compression level 5, is very slight worse than dfast in ratio, very slightly better in speed
- dfast has an standout performance/compression ratio at compression level 1

fast
falls apart starting at 4 or 5, dfast is always better
18% high
accelerometer_pyzstd_00_strat1fast	18.87%	1.64(sec)	559.29	MB/s encode	1688.62	MB/s decode  # weird
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


# some gyro data....
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
    output_name = data_stream + "_pyzstd_" + comp_disp(comp_level) + "_strat" + str(int(strat)) + PYZSTD_STRAT_NAMES[strat]
    
    # the zstd dictionary miiiight be useful if we can make it good, but it cuts speed in half,
    # and basic tests show it doesn't improve compression at all. Woo!
    thang = pyzstd.RichMemZstdCompressor(
        { # type: ignore
            pyzstd.CParameter.compressionLevel: comp_level,
            
            pyzstd.CParameter.nbWorkers: -1,  # -1 will do something that tends to keep it 1 core, but is better than 1?? 
            # positive integers may not actaully result in 1 full core of utilization.
            # 0 and -1 seem to do some kind of auto-detection that keeps a core fully busy.
            # documentation of zstd generally implies that there is a limit applied to small files
            
            pyzstd.CParameter.strategy: strat,  # its the strat
            
            # pyzstd.CParameter.jobSize: 2000000,  # some lower values sometimes reduce compression
            # pyzstd.CParameter.targetCBlockSize: 32768,  # should be 1340 <= value <= 131072
            # 0 seems to be default, it seems to be the best? 131072 has same compressionbut is slower
            # pyzstd.CParameter.contentSizeFlag: 160000  # no apparent effect
            # pyzstd.CParameter.enableLongDistanceMatching: 0,  # valid is 0,1,2; 1 reduces speed? doesn't do anything?
        },
        # the_zstd_dictionary,
    )
    
    t1 = perf_counter()
    output = thang.compress(content)
    t1 = perf_counter() - t1
    
    
    try:
        t2 = perf_counter()
        decompressed_output = pyzstd.decompress(output)
        # decompressed_output = pyzstd.decompress(output, the_zstd_dictionary)
        t2 = perf_counter() - t2
    except Exception:  # never had this error
        print(f"\n'{content}'")
        raise
    
    assert decompressed_output == content
    running_time[output_name] += t1
    running_counts[output_name] += len(output)
    running_time[output_name + "_dec"] += t2
    running_counts[output_name + "_dec"] += len(decompressed_output)


""" even faster, but possibly problematic
- need to determine whether this actually creates zstd standard binaries that can be decompressed by other libraries.  it might not?
- there are 3rd party additional compression options that _I cannot get working_
- has multiple compression options.

Blosc may be better once we get to binary compression formats
blosc allows only 9 compression levels and many other options
Blosc is oriented towards in-memory computation scenarios where memory bandwidth is the limit, not
    compute.  It may not create files that conform to the zstd spec.  Could be a preblem.

accelerometer_blosc_zstd_01_NOFIL_ts8_blk134217792	18.84%	1.57(sec)	583.36	MB/s    1687.69 MB/s decode
accelerometer_blosc_zstd_02_NOFIL_ts8_blk134217792	18.56%	1.94(sec)	472.92	MB/s    1767.43 MB/s decode
accelerometer_blosc_zstd_03_NOFIL_ts8_blk134217792	18.14%	4.4(sec)	208.23	MB/s    1766.57 MB/s decode
accelerometer_blosc_zstd_04_NOFIL_ts8_blk134217792	17.67%	7.02(sec)	130.41	MB/s    1850.65 MB/s decode
accelerometer_blosc_zstd_05_NOFIL_ts8_blk134217792	17.11%	9.57(sec)	95.6	MB/s    1966.04 MB/s decode
accelerometer_blosc_zstd_06_NOFIL_ts8_blk134217792	16.8%	17.39(sec)	52.64	MB/s    2011.77 MB/s decode
accelerometer_blosc_zstd_07_NOFIL_ts8_blk134217792	16.54%	30.19(sec)	30.31	MB/s    1999.54 MB/s decode
accelerometer_blosc_zstd_08_NOFIL_ts8_blk134217792	16.29%	38.21(sec)	23.95	MB/s    2032.76 MB/s decode
accelerometer_blosc_zstd_09_NOFIL_ts8_blk134217792	14.42%	180.55(sec)	5.07	MB/s    2031.48 MB/s decode
"""
def compress_blosc(
    data_stream, comp_level, typesize, block_size, use_dict, content, running_time,
    running_counts, meta
):
    for a_filter in BLOSC_FILTERS:
        key = data_stream + "_blosc_zstd_" + comp_disp(comp_level) \
                + "_" + a_filter.name[:5] + "_ts" + str(typesize) + "_blk" + str(block_size) \
                # + "_d" + str(use_dict) + "_m" + comp_disp(meta)
        # print(key, comp_level, a_filter, typesize, block_size, use_dict, meta)
        t1 = perf_counter()
        output = blosc2.compress2(
            content,
            clevel=comp_level,
            
            # filters are really confusing, but they probably are not well suited to raw csv data,
            # they are intended for compression of binary data and apply a binary transformation directly
            # to a numpy array (or whatever) before compression.
            # The input format of a set of ints is weird, I don't know where that came from.
            # Any value other than 0 (no filter) ruins reduces compression ratio substantially.
            # I don't even know what meta is.
            
            filters={0, 0, 0, 0, 0, 0},  
            # filters={a_filter.value, a_filter.value, a_filter.value, a_filter.value, a_filter.value, a_filter.value},  # any value not 0 (no filter) kill compression ratio
            # filters={0, a_filter.value, 0, 0, 0, 0},  # no effect?
            filters_meta={0, 0, 0, 0, 0, 0},  # no effect?
            codec=blosc2.Codec.ZSTD,
            # codec=blosc2.Codec.ZLIB,       # comparable to zstd, but slightly worse and always slower
            # codec=blosc2.Codec.LZ4HC,      # not as good compression, ~30%, but faster at 700 MB/s / 4000 MB/s.
            # codec=blosc2.Codec.LZ4,        # ignores compression level? Fast.
            # codec=blosc2.Codec.BLOSCLZ,    # VERY FAST. VERY NOT GOOD COMPRESSION
            # codec=blosc2.Codec.NDLZ,       # crash
            # codec=blosc2.Codec.ZFP_ACC,    # no compression
            # codec=blosc2.Codec.ZFP_PREC,   # no compression
            # codec=blosc2.Codec.ZFP_RATE,   # no compression
            # codec=blosc2.Codec.OPENHTJ2K,  # no compression
            # codec_meta=200,            # no apparent effect on zstd
            typesize=typesize,  # at some point I seem to have decided on 8 - possbibly due to raw csv data being utf-8?
            nthreads=1,    # no effect when block size is non-zero and block size is large
            blocksize=block_size,
            use_dict=False,
            # splitmode=blosc2.NEVER_SPLIT,   # no clue what this does.
        )
        t1 = perf_counter() - t1
        
        t2 = perf_counter()
        decompressed_output1 = blosc2.decompress(output)
        t2 = perf_counter() - t2
        assert decompressed_output1 == content
        
        # really weird code for how we print stats sorry
        running_time[key] += t1
        running_counts[key] += len(output)
        running_time[key+"_dec"] += t2
        running_counts[key + "_dec"] += len(decompressed_output1)


# if you want to play around with creating a zstd dictionary for pyzstd do this and then read it in
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

# with open("the_zstd_dictionary", "rb") as f:
#     loaded_dict_content = f.read()
#     the_zstd_dictionary = pyzstd.ZstdDict(loaded_dict_content)

# main_pyzstd()
# main_zstd()
# main_blosc()
# main_zstandard()
run_both()
