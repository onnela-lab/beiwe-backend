import pyzstd
from config.settings import DATA_COMPRESSION_LEVEL
# there has been substantial testing of the zstd compression modes for data produced by beiwe.
# the pyzstd library implementation of zstd is the fastest one based on benchmarks.

def compress(some_bytes: bytes, level=DATA_COMPRESSION_LEVEL) -> bytes:
    if level > 4:
        raise Exception(
            "the 'dfast' strategy does not work correctly with compression levels 0-4, see code in the compression_tests for more details."
        )
    return pyzstd.RichMemZstdCompressor(  # type: ignore
        {
            pyzstd.CParameter.compressionLevel: level,
            pyzstd.CParameter.nbWorkers: -1,
            # positive integers may not actually result in 1 full core of utilization.
            # 0 and -1 seem to do some kind of auto-detection that keeps a core fully busy.
            # documentation of zstd generally implies that there is a limit applied to small files
            pyzstd.CParameter.strategy: pyzstd.Strategy.dfast,
        }
    ).compress(some_bytes)


def decompress(input: bytes):
    return pyzstd.decompress(input)
