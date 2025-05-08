import asyncio
import logging

logger = logging.getLogger(__name__)

class KeyValueStore:

    """ Data storage engine. Return values are tuples
        in the form of (OK|DATA) or (ERR|MSG)
    """

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        """ Expose the internal lock for atomic ops 
            as an attribute """
        logger.debug("Acquiring lock.")
        return self._lock

    async def set(self, key: str, value: str) -> tuple[str, str]:
        """Set the value for a given key."""
        async with self._lock:
            try:
                self._store[key] = value
                logger.debug(f"Data Dump: {self._store}")
                return "OK", None
            except:
                return "ERR", "Failed to update key."

    async def get(self, key: str) -> tuple[str, str]:
        """Get the value of a given key."""
        print(f"{self._store}")
        async with self._lock:
            try:
                value = self._store.get(key,None)
                logger.debug(f"Data Dump: {self._store}")
                if value:
                    return "OK", value
                else:
                    return "ERR", "Key does not exist."
            except:
                return "ERR", "Failed to retrieve key."

    async def delete(self, key: str) -> tuple[str, str]:
        """Delete the given key."""
        logger.debug(f"DELETE key: {key}")
        async with self._lock:
            try:
                value = self._store.pop(key,None)
                logger.debug(f"Data Dump: {self._store}")
                if value:
                    return "OK", None
                else:
                    return "ERR", "Key does not exist."
            except:
                return "ERR", "Failed to delete key."

    # INTERNAL: these assume the caller already holds a lock.
    # This is used for transactions. Proceed at your own risk.

    def _set_nolock(self, key: str, value: str) -> None:
        """ Set without re-acquiring the lock. """
        self._store[key] = value
        logger.debug(f"Data Dump: {self._store}")

    def _delete_nolock(self, key: str, value: str) -> None:
        """ Delete without reacquiring the lock. """
        self._store.pop(key, None)
        logger.debug(f"Data Dump: {self._store}")
