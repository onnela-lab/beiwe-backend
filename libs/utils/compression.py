from config.settings import DATA_COMPRESSION_LEVEL
from backports.zstd import Strategy, compress as _compress, CompressionParameter, decompress as _decompress

decompress = _decompress  # its imported everywhere

# there has been substantial testing of the zstd compression modes for data produced by beiwe.

def compress(some_bytes: bytes, level: int = DATA_COMPRESSION_LEVEL) -> bytes:
    # the dfast strategy, for low compression levels, are substantially better for our bulk data.
    strat = {} if level > 4 else {CompressionParameter.strategy: Strategy.dfast}
    
    return _compress(  # type: ignore
        some_bytes, None, {
            CompressionParameter.compression_level: level,
            # positive integers may not actually result in 1 full core of utilization. 0 and -1 seem
            # to do some kind of auto-detection that keeps a core fully busy. documentation of zstd
            # generally implies that there is a limit applied to small files.
            CompressionParameter.nb_workers: 1,
            # Our data is not actually improved by long distance matching, generic data tends to be.
            CompressionParameter.enable_long_distance_matching: 0,
            **strat
        }
    )
