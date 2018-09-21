#!/usr/bin/env python

import shlex
import logging
import asyncio
import argparse
import datetime

log = logging.getLogger(__name__)

tcp_buffer_size = 2048
max_maintenance_command_size = tcp_buffer_size

flush_to = asyncio.Future()
servers = set()
connections = []


class Connection:
    opened_at = None
    forwarded_at = None
    completed_at = None
    closed_at = None


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--log-level', default='INFO')
    arg_parser.add_argument('--port', type=int, default=8888)
    arg_parser.add_argument('--maintenance-port', type=int, default=None)
    arg_parser.add_argument('--maintenance-unix-path', default=None)
    args = arg_parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level)

    loop = asyncio.get_event_loop()

    server = loop.run_until_complete(asyncio.start_server(handle_client, port=args.port))
    log.info('Serving on %s', format_sockname(server.sockets[0].getsockname()))
    servers.add(server)

    if args.maintenance_port:
        server = loop.run_until_complete(asyncio.start_server(handle_maintenance_client, port=args.maintenance_port))
        servers.add(server)
        log.info('Serving maintenance on %s', format_sockname(server.sockets[0].getsockname()))

    if args.maintenance_unix_path:
        path = args.maintenance_unix_path
        server = loop.run_until_complete(asyncio.start_unix_server(handle_maintenance_client, path))
        servers.add(server)
        log.info('Serving maintenance on %s', path)

    try:
        loop.run_until_complete(asyncio.gather(*(s.wait_closed() for s in servers)))
    except KeyboardInterrupt:
        pass

    loop.close()
    print_stats()


def finish_serving():
    for server in servers:
        server.close()
    log.info('Not accepting new requests')
    log.info('Waiting for buffered connections to close')


async def handle_client(in_reader, in_writer):
    conn = Connection()
    conn.opened_at = datetime.datetime.now()
    connections.append(conn)

    log.info('Buffered connection from %s', format_sockname(in_writer.get_extra_info('peername')))

    try:
        host, port = await flush_to
        out_reader, out_writer = await asyncio.open_connection(host, port)

        conn.forwarded_at = datetime.datetime.now()
        log.info('Forwarding connection from %s to %s',
                 format_sockname(in_writer.get_extra_info('peername')),
                 format_sockname(out_writer.get_extra_info('peername')))

        pipe1 = pipe(in_reader, out_writer)
        pipe2 = pipe(out_reader, in_writer)
        await asyncio.gather(pipe1, pipe2)

        conn.completed_at = datetime.datetime.now()
        log.info('Connection from %s to %s forwarded',
                 format_sockname(in_writer.get_extra_info('peername')),
                 format_sockname(out_writer.get_extra_info('peername')))
    finally:
        in_writer.close()
        conn.closed_at = datetime.datetime.now()


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
        argv = shlex.split(await read_command(reader))

        parser = MyArgParser(prog='')

        subparsers = parser.add_subparsers(dest='command')
        flush_parser = subparsers.add_parser('flush')
        flush_parser.add_argument('host')
        flush_parser.add_argument('port', type=int)

        args = vars(parser.parse_args(argv))
        log.info('Got %s from %s', args, format_sockname(writer.get_extra_info('peername')))

        await globals()['handle_command_' + args.pop('command')](**args)

        writer.write(b'OK\n')

    except Exception:
        writer.write(b'Error\n')

    finally:
        writer.close()


async def handle_command_flush(host, port):
    finish_serving()
    flush_to.set_result((host, port))


async def read_command(reader):
    data = b''

    while not reader.at_eof():
        if len(data) >= max_maintenance_command_size:
            raise Exception(f'Maintenance command starting with "{data}" is too long')
        data += await reader.read(tcp_buffer_size)

    return data.strip().decode()


def format_sockname(sockname):
    return '{}:{}'.format(*sockname)


def print_stats():
    total = len(connections)
    completed = sum(1 for c in connections if c.completed_at)
    log.info('Total of %s connections were buffered', total)
    if total:
        log.info('%s(%s%%) were successfully flushed', completed, int(completed / total * 100))

    time_in_buffer = [(c.forwarded_at - c.opened_at).total_seconds() for c in connections if c.forwarded_at]
    if time_in_buffer:
        max_time_in_buffer = max(time_in_buffer)
        avg_time_in_buffer = sum(time_in_buffer) / len(time_in_buffer)
        log.info('Time being buffered: max %s s, avg %s s', int(max_time_in_buffer), int(avg_time_in_buffer))


if __name__ == '__main__':
    main()
