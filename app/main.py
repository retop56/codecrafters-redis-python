import asyncio
from collections import deque
import time
from typing import Optional

command_queue = deque()
key_store: dict[str, tuple[str, Optional[float]]] = {}
"""key --> (value, Optional[expiry])"""


def handle_get_command(writer: asyncio.StreamWriter) -> None:
    key = decode_simple_string()
    if key not in key_store:
        writer.write("$-1\r\n".encode())
        return
    val, expiry = key_store[key]
    if expiry is None:
        writer.write(f"${len(val)}\r\n{val}\r\n".encode())
        return
    if time.time() > expiry:
        writer.write("$-1\r\n".encode())
        return
    writer.write(f"${len(val)}\r\n{val}\r\n".encode())


def handle_set_command(writer: asyncio.StreamWriter) -> None:
    key = decode_simple_string()
    val = decode_simple_string()
    print(f"Decoded key-val: {key} --> {val}")
    print(f"Command queue after decoding key-val: {command_queue}")
    if len(command_queue) > 1 and command_queue[1].lower() == "px":
        decode_simple_string()
        expiry_length_seconds = float(decode_simple_string()) / 1000
        expiry_time = time.time() + expiry_length_seconds
    else:
        expiry_time = None
    key_store[key] = (val, expiry_time)
    print(f"Set {key} to {(val, expiry_time)}")
    writer.write("+OK\r\n".encode())


def handle_echo_command(writer: asyncio.StreamWriter) -> None:
    c = decode_simple_string()
    writer.write(f"${len(c)}\r\n{c}\r\n".encode())


def decode_simple_string() -> str:
    # Remove $<length-of-string>
    command_queue.popleft()
    return command_queue.popleft()


def decode_array(writer: asyncio.StreamWriter) -> None:
    # Turn "*<number>" into integer, then check whether we have that number of elements
    # in command queue. If not, throw it back to connection handler to continue
    # reading in bytes over connection into queue
    expected_array_length = int(command_queue.popleft()[1:])
    if (expected_array_length * 2) < len(command_queue):
        return

    if command_queue[0].startswith("$"):
        s = decode_simple_string()
        match s:
            case "PING":
                writer.write("+PONG\r\n".encode())
            case "ECHO":
                handle_echo_command(writer)
            case "SET":
                handle_set_command(writer)
            case "GET":
                handle_get_command(writer)


async def connection_handler(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    while True:
        data = await reader.read(100)
        print(f"Received {data}")
        # Remove any empty strings from message after splitting
        data = [chunk for chunk in data.decode().split("\r\n") if chunk]
        for d in data:
            command_queue.append(d)
        print(f"Updated command queue: {command_queue}")

        # Checking first character of first item in command queue
        match command_queue[0][0]:
            case "*":
                decode_array(writer)
            case _:
                raise ValueError("Huh?")


async def main():
    server_socket = await asyncio.start_server(connection_handler, "localhost", 6379)
    async with server_socket:
        await server_socket.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
