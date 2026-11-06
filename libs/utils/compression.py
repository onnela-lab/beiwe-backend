from backports.zstd import (CompressionParameter, decompress as _decompress, get_frame_info, Strategy,
    ZstdCompressor, ZstdDecompressor)

from config.settings import DATA_COMPRESSION_LEVEL


# there has been substantial testing of the zstd compression modes for data produced by beiwe.

def _compression_options(level: int) -> dict:
    # the dfast strategy, for low compression levels, are substantially better for our bulk data.
    strat = {} if level > 4 else {CompressionParameter.strategy: Strategy.dfast}
    return {
        CompressionParameter.compression_level: level,
        # positive integers may not actually result in 1 full core of utilization. 0 and -1 seem
        # to do some kind of auto-detection that keeps a core fully busy. documentation of zstd
        # generally implies that there is a limit applied to small files.
        CompressionParameter.nb_workers: 1,
        # Our data is not actually improved by long distance matching, generic data tends to be.
        CompressionParameter.enable_long_distance_matching: 0,
        **strat
    }


def compress(some_bytes: bytes, level: int = DATA_COMPRESSION_LEVEL) -> bytes:
    """ Compresses data into a single zstd frame.
    
    backports.zstd.compress allocates an output buffer of the worst-case compressed size and later
    shrinks it. CONTINUE mode grows the output as needed instead (for a 52 MB csv the peak drops
    from +63 MB to +22 MB). We use the uncompressed size in the frame header. """
    compressor = ZstdCompressor(options=_compression_options(level))
    compressor.set_pledged_input_size(len(some_bytes))
    return compressor.compress(some_bytes, mode=ZstdCompressor.CONTINUE) + compressor.flush(ZstdCompressor.FLUSH_FRAME)


def decompress(data: bytes) -> bytes:
    """ Decompresses zstd data, accepts any bytes-like object.
    
    Our files record uncompressed size in the frame header, providing it has peak 2x memory use.
    Frames without a recorded size or with multiple frames use the normal path, which outputs in
    blocks and then copies them into the result, peak around 2.5x of decompressed size. """
    size = get_frame_info(data).decompressed_size
    if not size:  # None (not recorded) or 0
        return _decompress(data)
    
    decompressor = ZstdDecompressor()
    result = decompressor.decompress(data, max_length=size)
    if decompressor.eof and not decompressor.unused_data:
        return result
    return _decompress(data)  # multiple frames, or the header size was wrong
