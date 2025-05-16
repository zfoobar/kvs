import asyncio
import logging

logger = logging.getLogger(__name__)

class KeyValueStore:

    """ Data storage engine. Return values are tuples
        in the form of (OK|DATA) or (ERR|MSG)
    """

    def __init__(self) -> None:
        self._store: dict[str, dict] = {}
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
                # if the key exists, increment version
                # otherwise store the key with version set to 1.
                if key in self._store:
                    self._store[key]['version'] += 1
                    self._store[key]['value'] = value
                else:
                    self._store[key] = {}
                    self._store[key]['value'] = value
                    self._store[key]['version'] = 1

                logger.debug(f"Data Dump: {self._store}")
                return "OK", None
            except Exception as e:
                return "ERR", f"Failed to update key: {e}"

    async def get(self, key: str) -> tuple[str, str]:
        """Get the value of a given key."""
        print(f"{self._store}")
        async with self._lock:
            data = self._store.get(key,None)
            try:
                value = data['value']
                version = data['version']
                logger.debug(f"Data Dump: {self._store}")
                if value:
                    return "OK", data
                else:
                    return "ERR", "Key does not exist."
            except Exception as e:
                return "ERR", f"Failed to retrieve key. {e}"

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

    def _get_nolock(self, key: str) -> None:
        """ GET without re-acquiring the lock. """
        logger.debug(f"Data Dump: {self._store}")
        data = self._store.get(key,None)
        return data

    def _set_nolock(self, key: str, value: str) -> None:
        """ Set without re-acquiring the lock. """
        try:
            # if the key exists, increment version
            # otherwise store the key with version set to 1.
            if key in self._store:
                self._store[key]['version'] += 1
                self._store[key]['value'] = value
            else:
                self._store[key] = {}
                self._store[key]['value'] = value
                self._store[key]['version'] = 1

            logger.debug(f"Data Dump: {self._store}")
            return "OK", None
        except Exception as e:
            return "ERR", f"Failed to update key: {e}"

    def _delete_nolock(self, key: str, value: str) -> None:
        """ Delete without reacquiring the lock. """
        self._store.pop(key, None)
        logger.debug(f"Data Dump: {self._store}")
