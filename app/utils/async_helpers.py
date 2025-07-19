from typing import AsyncGenerator

class AsyncGeneratorReader:
    """
    Adapts an async generator that yields bytes into an async file-like
    object with an async read() method that ijson can use.
    """
    def __init__(self, generator: AsyncGenerator[bytes, None]):
        self.generator = generator
        self.buffer = bytearray()

    async def read(self, size: int = -1) -> bytes:
        while len(self.buffer) < size or size == -1:
            try:
                chunk = await self.generator.__anext__()
                self.buffer.extend(chunk)
            except StopAsyncIteration:
                break # Generator is exhausted

        if size == -1:
            data = self.buffer
            self.buffer = bytearray()
            return bytes(data)
        
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return bytes(data)
