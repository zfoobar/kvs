import asyncio
from enum import Enum
from store import KeyValueStore
from protocol import CommandProcessor

class KeyValueServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 8888) -> None:
        self.store = KeyValueStore()
        self.processor = CommandProcessor(self.store)
        self.host = host
        self.port = port
        self._session_counter = 0
        self._server = None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle communication with a single client."""
        addr = writer.get_extra_info('peername')
        self._session_counter +=1
        session_id = f"session_{self._session_counter}"
        print(f"Client connected: {addr}")

        try:
            while not reader.at_eof():
                data = await reader.readline()
                if not data:
                    break
                try:
                    message = data.decode('utf-8').strip()
                except UnicodeDecodeError:
                    writer.write(b"ERROR: Invalid UTF-8 sequence\n")
                    await writer.drain()
                    return
                if not message:
                    continue
                response = await self.processor.process(message, session_id)
                writer.write((response + '\n').encode('utf-8'))
                await writer.drain()
        except ConnectionResetError:
            pass
        finally:
            print(f"Client disconnected: {addr}")
            writer.close()
            await writer.wait_closed()

    async def start(self) -> None:
        """ Start the server and handle incoming connections.   """
        self._server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = self._server.sockets[0].getsockname()
        print(f"Serving on {addr}")

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """ Stop the server gracefully """
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            print("Server stopped.")
        else:
            print("Server not running.")
