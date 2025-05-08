import asyncio
from kvs.helpers import setup_logging
from kvs.server import KeyValueServer

# our entrypoint for KVS
if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(KeyValueServer().start())
    except KeyboardInterrupt:
        print("Server stopped.")
