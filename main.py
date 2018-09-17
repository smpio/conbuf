import asyncio

flush_read_buffer = 2048


def main():
    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(handle_client, '127.0.0.1', 8888)
    server = loop.run_until_complete(coro)

    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


async def handle_client(local_reader, local_writer):
    try:
        remote_reader, remote_writer = await asyncio.open_connection(
            '127.0.0.1', 8889)
        pipe1 = pipe(local_reader, remote_writer)
        pipe2 = pipe(remote_reader, local_writer)
        await asyncio.gather(pipe1, pipe2)
    finally:
        local_writer.close()


async def pipe(reader, writer):
    try:
        while not reader.at_eof():
            writer.write(await reader.read(flush_read_buffer))
    finally:
        writer.close()


if __name__ == '__main__':
    main()
