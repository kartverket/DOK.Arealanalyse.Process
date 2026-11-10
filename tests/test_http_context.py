import asyncio
from dokanalyse.utils import http_context


async def test_session_is_shared_and_can_be_closed():
    session = http_context.get_session()

    assert http_context.get_session() is session

    await http_context.close_session()

    assert session.closed
    assert http_context.get_session() is not session

    await http_context.close_session()


def test_new_session_for_another_event_loop():
    async def get_and_close():
        session = http_context.get_session()
        await http_context.close_session()
        return session

    first = asyncio.run(get_and_close())
    second = asyncio.run(get_and_close())

    assert first is not second
