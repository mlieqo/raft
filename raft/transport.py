import asyncio
import logging


logger = logging.getLogger(__name__)
HEADER_SIZE = 10


async def send_message(writer: asyncio.StreamWriter, msg: bytes) -> None:
    header = b'%10d' % len(msg)
    writer.write(header + msg)
    try:
        await writer.drain()
    except Exception as e:
        logger.error('Error - %s, while sending message - %s', e, msg)
        raise e


async def recv_message(reader: asyncio.StreamReader) -> bytes:
    # let this raise exc if not complete message
    msg_length = int(await reader.readexactly(HEADER_SIZE))
    full_message = await reader.readexactly(msg_length)
    return full_message
