from store import KeyValueStore
from enum import Enum
import json
import logging

logger = logging.getLogger(__name__)

class CommandProcessor:
    def __init__(self, store: KeyValueStore) -> None:
        self.store = store
        self.transactions = {}

    class _StatusMessage(Enum):
        """ Explicitly enumerate our message types """

        SUCCESS = "Ok"
        ERROR = "Error"

    def _json_response(self, status: str, data: dict = None, mesg: str = None) -> str:
        """Helper method to create JSON response."""

        response = {
            "status": status.value,
        }

        if data:
            response["result"] = data

        response["message"] = mesg if mesg else None

        return json.dumps(response)

    def _error(self, mesg: str) -> str:
        """ Error wrapper. """
        return self._json_response(self._StatusMessage.ERROR, None, mesg)

    def _success(self, data, mesg):
        """ Success wrapper. """
        return self._json_response(self._StatusMessage.SUCCESS, data, mesg)

    async def process(self, message: str, session_id: str = None) -> str:
        """Process the command from the client."""
        try:
            parts = message.strip().split()
            cmd = parts[0].upper()

            # Default to optimism (famous last words)
            command_status = self._StatusMessage.SUCCESS
            data = None

            if cmd == "START":
                logger.debug(f"START issued from session_id: {session_id} {self.transactions}")
                if session_id in self.transactions:
                    return self._error("Already in transaction.")
                # start buffering commands
                # and initialize keys and ops dictionary
                self.transactions[session_id] = {}
                self.transactions[session_id]['keys'] = {}
                self.transactions[session_id]['ops'] = []
                return self._success(None, "Transaction Started.")

            # in our logic, COMMIT only handles mutations to the data.
            # GETs are never deferred, but they do read from the transaction
            # buffer.

            elif cmd == "COMMIT":
                if session_id not in self.transactions:
                    return self._error("No transaction in progress")

                # Acquire global lock for atomicity.
                async with self.store.lock:
                    # have any of the keys we've buffered changed since
                    # they were touched?
                    for key in self.transactions[session_id]['keys']:
                        current_version = self.store._get_nolock(key)['version']
                        buffered_version = self.transactions[session_id]['keys'][key]
                        if current_version != buffered_version:
                            del self.transactions[session_id]
                            return self._error("Key version has changed since we last touched the key")

                    for op, key, value in self.transactions[session_id]['ops']:
                        if op == "PUT":
                            self.store._set_nolock(key,value)
                        elif op == "DEL":
                            self.store._delete_nolock(key,value)
                del self.transactions[session_id]
                return self._success(None, "Transaction committed.")

            elif cmd == "ROLLBACK":
                if session_id not in self.transactions:
                    return self._error("No transaction in progress")
                self.transactions.pop(session_id) 
                return self._success(None, "Transaction rolled back.")

            elif cmd == "PUT":
                if len(parts) >= 3:
                    key = parts[1]
                    value = ' '.join(parts[2:])
                    # if we're inside a transaction, buffer - otherwise
                    # otherwise immediately execute
                    if session_id in self.transactions:
                        self.transactions[session_id]['ops'].append(("PUT",key,value))
                        # get version of the key
                        store_status, data = await self.store.get(key)
                        print(f"DATA: {data}")
                        if store_status == "OK":
                            version = data["version"]
                        else:
                            # key doesn't exist, init version to 1
                            version = 1
                        self.transactions[session_id]['keys'][key] = version
                        logger.debug(f"Current transaction buffer: {self.transactions}")
                        return self._success(None, f"PUT buffered for key '{key}'.")
                    store_status, data = await self.store.set(key, value)
                else:
                    return self._error("PUT expects at least 3 arguments.")

            elif cmd == "GET":
                if len(parts) == 2:
                    key = parts[1]
                    # If we're inside a transaction, GET should be
                    # consistent with the buffered transaction operations.
                    if session_id in self.transactions:
                        # Search backwards for latest buffered op
                        for op, k, v in reversed(self.transactions[session_id]['ops']):
                            if k == key:
                                if op == "PUT":
                                    return self._success(v, "GET from transaction buffer")
                                elif op == "DEL":
                                    return self._error(f"Key {k} was deleted in this transaction")
                    # No buffered op/key pair found, read through store
                    store_status, data = await self.store.get(key)
                    logger.debug(f"GET returned: {data}")
                else:
                    return self._error("GET expect 2 arguments")

            elif cmd == "DEL":
                if len(parts) == 2:
                    key = parts[1]
                    if session_id in self.transactions:
                        self.transactions[session_id].append(("DEL", key, None))
                        return self._success(None, f"DELETE buffered for key: '{key}'.")
                    store_status, data = await self.store.delete(key)
                else:
                    return self._error("DELETE expects 2 arguments")

            else:
                return self._error("Invalid command.")

            # We expect tuples (OK|Data) or (Err|Message)
            # from the storage engine. If there is an error,
            # the data is the message.
            if store_status == "OK":
                return self._success(data, "Command succeeded.")
            else:
                return self._error(data)

        except Exception as e:
            return f"ERROR: {str(e)}"
