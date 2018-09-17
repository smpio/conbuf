import logging
import asyncio
import argparse

tcp_buffer_size = 2048
max_maintenance_command_size = tcp_buffer_size
log = logging.getLogger(__name__)
queues = set()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--log-level', default='WARNING')
    args = arg_parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level)

    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(asyncio.start_server(handle_client, '127.0.0.1', 8888))
    maintenance_server = loop.run_until_complete(asyncio.start_server(handle_maintenance_client, '127.0.0.1', 8889))

    print('Serving on {}'.format(server.sockets[0].getsockname()))
    print('Serving maintenance on {}'.format(maintenance_server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    maintenance_server.close()
    loop.run_until_complete(asyncio.gather(server.wait_closed(), maintenance_server.wait_closed()))
    loop.close()


async def handle_client(local_reader, local_writer):
    q = asyncio.Queue()
    queues.add(q)

    try:
        host, port = await q.get()
        remote_reader, remote_writer = await asyncio.open_connection(host, port)
        pipe1 = pipe(local_reader, remote_writer)
        pipe2 = pipe(remote_reader, local_writer)
        await asyncio.gather(pipe1, pipe2)
    finally:
        local_writer.close()


async def handle_maintenance_client(reader, writer):
    command = b''

    while not reader.at_eof():
        if len(command) >= max_maintenance_command_size:
            log.error('Maintenance command starting with "%s" is too long', command)
            writer.close()
            return
        command += await reader.read(tcp_buffer_size)

    command = command.strip()

    if command == b'qwe':
        flush('127.0.0.1', 8887)
    else:
        log.error('Invalid command: "%s"', command)

    writer.close()


async def pipe(reader, writer):
    try:
        while not reader.at_eof():
            writer.write(await reader.read(tcp_buffer_size))
    finally:
        writer.close()


def flush(host, port):
    for queue in queues:
        queue.put_nowait((host, port))


if __name__ == '__main__':
    main()
