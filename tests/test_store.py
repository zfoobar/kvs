import pytest
import asyncio

from store import KeyValueStore
from protocol import CommandProcessor

@pytest.fixture(scope="module", autouse=True)
def store():
    return KeyValueStore()

@pytest.fixture(scope="module", autouse=True)
def processor(store):
    return CommandProcessor(store)

@pytest.mark.asyncio
async def test_non_transactional_put_get(processor):
    session = "non_tx"
    # PUT without START applies immediately
    resp_put = await processor.process("PUT foo 123", session)
    assert '"status": "Ok"' in resp_put
    # GET should return the new value
    resp_get = await processor.process("GET foo", session)
    assert '"result": "123"' in resp_get

@pytest.mark.asyncio
async def test_get_missing_key(processor):
    session = "default"
    resp = await processor.process("GET missing", session)
    assert '"status": "Error"' in resp
    assert '"Key does not exist."' in resp

@pytest.mark.asyncio
async def test_overwrite_key(processor):
    session = "default"
    await processor.process("PUT foo original", session)
    resp = await processor.process("PUT foo updated", session)
    assert '"status": "Ok"' in resp
    resp = await processor.process("GET foo", session)
    assert '"result": "updated"' in resp

@pytest.mark.asyncio
async def test_delete_missing_key(processor):
    session = "default"
    resp = await processor.process("DEL ghost", session)
    assert '"status": "Error"' in resp
    assert "Key does not exist." in resp

@pytest.mark.asyncio
async def test_delete_key(processor):
    session = "default"
    await processor.process("PUT delete_me value", session)
    resp = await processor.process("DEL delete_me", session)
    assert '"status": "Ok"' in resp
    resp = await processor.process("GET delete_me", session)
    assert '"status": "Error"' in resp

@pytest.mark.asyncio
async def test_transaction_commit(processor):
    session = "tx1"
    await processor.process("START", session)
    await processor.process("PUT alpha 1", session)
    await processor.process("PUT beta 2", session)
    resp = await processor.process("COMMIT", session)
    assert '"status": "Ok"' in resp
    resp = await processor.process("GET alpha", session)
    assert '"result": "1"' in resp

@pytest.mark.asyncio
async def test_transaction_isolation(processor):
    session_a = "A"
    session_b = "B"
    await processor.process("START", session_a)
    await processor.process("PUT x value-a", session_a)

    await processor.process("START", session_b)
    await processor.process("PUT x value-b", session_b)

    resp = await processor.process("GET x", session_a)
    assert '"result": "value-a"' in resp
    resp = await processor.process("GET x", session_b)
    assert '"result": "value-b"' in resp

@pytest.mark.asyncio
async def test_nested_transaction_error(processor):
    session = "tx_nested"
    await processor.process("START", session)
    resp = await processor.process("START", session)
    assert '"status": "Error"' in resp
    assert "Already in transaction" in resp

@pytest.mark.asyncio
async def test_transactional_get_reads_staged_value(processor):
    session = "tx_read"
    await processor.process("PUT foo original", session)
    await processor.process("START", session)
    await processor.process("PUT foo modified", session)
    resp = await processor.process("GET foo", session)
    assert '"result": "modified"' in resp
@pytest.mark.asyncio
async def test_rollback_discards_changes(processor):
    session = "tx_rb"
    # Start and buffer a change
    await processor.process("START", session)
    await processor.process("PUT x 999", session)
    # Rollback instead of commit
    resp_rb = await processor.process("ROLLBACK", session)
    assert "Transaction rolled back" in resp_rb

    # GET now should not see the buffered change
    resp_get = await processor.process("GET x", session)
    assert '"status": "Error"' in resp_get or '"result"' not in resp_get

@pytest.mark.asyncio
async def test_concurrent_commits_do_not_interleave(processor):
    session1 = "c1"
    session2 = "c2"
    # Prepare two transactions
    await processor.process("START", session1)
    await processor.process("START", session2)
    for i in range(1,1000):
       print(f"Putting key{i} in each session...")
       await processor.process(f"PUT key{i} 1", session1)
       await processor.process(f"PUT key{i} 2", session2)

    # Commit both
    await asyncio.gather(
        processor.process("COMMIT", session1),
        processor.process("COMMIT", session2),
    )

    # Exactly one of the two values must win across all keys, but no partial state
    for i in range(1,1000):
        print(f"Testing key{i} in session1...")
        # Exactly one of the two values must win across all keys, but no partial state
        resp = await processor.process(f"GET key{i}", session1)
        if i == 1:
            if '"result": "1"' in resp:
                v = 1
            else:
                v = 2
            print(f"Value initialized to {v}")

        # Value is either 1 or 2 for the entire key set
        assert f'"result": "{v}"' in resp
