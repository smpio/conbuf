import shlex
import logging
import asyncio
import argparse

tcp_buffer_size = 2048
max_maintenance_command_size = tcp_buffer_size
log = logging.getLogger(__name__)
queues = set()
servers = set()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--log-level', default='WARNING')
    args = arg_parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level)

    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(asyncio.start_server(handle_client, '127.0.0.1', 8888))
    maintenance_server = loop.run_until_complete(asyncio.start_server(handle_maintenance_client, '127.0.0.1', 8889))
    servers.add(server)
    servers.add(maintenance_server)

    print('Serving on {}'.format(server.sockets[0].getsockname()))
    print('Serving maintenance on {}'.format(maintenance_server.sockets[0].getsockname()))

    loop.run_until_complete(asyncio.gather(*(s.wait_closed() for s in servers)))
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


async def pipe(reader, writer):
    try:
        while not reader.at_eof():
            writer.write(await reader.read(tcp_buffer_size))
    finally:
        writer.close()


class MyArgParser(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        raise Exception(message)


async def handle_maintenance_client(reader, writer):
    try:
        data = b''

        while not reader.at_eof():
            if len(data) >= max_maintenance_command_size:
                raise Exception(f'Maintenance command starting with "{data}" is too long')
            data += await reader.read(tcp_buffer_size)

        argv = shlex.split(data.strip().decode())
        parser = MyArgParser(prog='')
        subparsers = parser.add_subparsers(dest='command')
        flush_parser = subparsers.add_parser('flush')
        flush_parser.add_argument('host')
        flush_parser.add_argument('port', type=int)

        args = vars(parser.parse_args(argv))
        await globals()['handle_command_' + args.pop('command')](**args)
        writer.write(b'OK\n')

    finally:
        writer.close()


async def handle_command_flush(host, port):
    for queue in queues:
        queue.put_nowait((host, port))
    for server in servers:
        server.close()


if __name__ == '__main__':
    main()
