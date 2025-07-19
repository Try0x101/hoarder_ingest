import zlib
import brotli
import zstandard
import aiofiles
from typing import AsyncGenerator, Optional

async def _chunked_decompressor(file_path: str, decompressor, chunk_size: int = 65536) -> AsyncGenerator[bytes, None]:
    async with aiofiles.open(file_path, 'rb') as f:
        while chunk := await f.read(chunk_size):
            yield decompressor.decompress(chunk)

async def get_decompressed_stream(file_path: str, content_encoding: Optional[str]) -> AsyncGenerator[bytes, None]:
    encoding = content_encoding.lower() if content_encoding else 'identity'

    if encoding == 'gzip':
        decompressor = zlib.decompressobj(32 + zlib.MAX_WBITS)
        return _chunked_decompressor(file_path, decompressor)
    
    elif encoding == 'deflate':
        decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        return _chunked_decompressor(file_path, decompressor)
        
    elif encoding == 'br':
        decompressor = brotli.Decompressor()
        return _chunked_decompressor(file_path, decompressor)

    elif encoding == 'zstd':
        decompressor = zstandard.ZstdDecompressor()
        return _chunked_decompressor(file_path, decompressor)

    else:
        async def _identity_streamer(chunk_size: int = 65536):
            async with aiofiles.open(file_path, 'rb') as f:
                while chunk := await f.read(chunk_size):
                    yield chunk
        return _identity_streamer()
