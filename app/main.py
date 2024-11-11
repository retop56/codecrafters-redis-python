import asyncio
from collections import deque
import time
from typing import Optional, BinaryIO
import argparse

command_queue = deque()
key_store: dict[str, tuple[str, Optional[float]]] = {}
"""key --> (value, Optional[expiry])"""


parser = argparse.ArgumentParser(
    prog="Bootleg Redis",
    description="'Build your own Redis' Codecrafters challenge",
)
parser.add_argument("--dir")
parser.add_argument("--dbfilename")
args = parser.parse_args()


def rdb_file_process_size_encoded_value(f: BinaryIO) -> tuple[int, BinaryIO]:
    first_byte = f.read(1)
    print(f"First byte: {first_byte}")
    first_two_bit_mask = 0b11000000
    bottom_six_bit_mask = 0b00111111
    bottom_fourteen_bit_mask = 0x3FFF
    first_two_bits = int.from_bytes(first_byte) & first_two_bit_mask
    print(f"First two bits: {first_two_bits}")
    match first_two_bits:
        # If the first two bits are 0b00:
        # The size is the remaining 6 bits of the byte.
        case 0b00000000:
            print("First two bits are 0b00")
            size_value = int.from_bytes(first_byte) & bottom_six_bit_mask
            return (size_value, f)
        # If the first two bits are 0b01:
        # The size is the next 14 bits
        # (remaining 6 bits in the first byte, combined with the next byte),
        # in big-endian (read left-to-right).
        case 0b01000000:
            print("First two bits are 0b01")
            size_value = first_byte + f.read(1)
            size_value = int.from_bytes(size_value) & bottom_fourteen_bit_mask
            return (size_value, f)
        # If the first two bits are 0b10:
        # Ignore the remaining 6 bits of the first byte.
        # The size is the next 4 bytes, in big-endian (read left-to-right).
        case 0b10000000:
            print("First two bits are 0b10")
            size_value = int.from_bytes(f.read(4))
            return (size_value, f)
        case _:
            raise ValueError(
                f"Unable to read size-encoded value. (First two bits: {first_two_bits})"
            )


def rdb_file_process_string_encoded_value(f: BinaryIO) -> tuple[str, BinaryIO]:
    first_byte = f.read(1)
    if (int.from_bytes(first_byte) & 0b11000000) == 0b11000000:
        match first_byte:
            case b"\xC0":
                s = str(int.from_bytes(f.read(1)))
                print(f'Found integer string: "{s}"')
                return (s, f)
            case b"\xC1":
                s = str(int.from_bytes(f.read(2), byteorder="little"))
                print(f'Found integer string: "{s}"')
                return (s, f)
            case b"\xC2":
                s = str(int.from_bytes(f.read(4), byteorder="little"))
                print(f'Found integer string: "{s}"')
                return (s, f)
            case _:
                raise ValueError(
                    "Unable to process string-encoded value"
                    f"(First byte: {first_byte}"
                )
    else:
        f.seek(-1, 1)
        length_of_string, f = rdb_file_process_size_encoded_value(f)
        print(f"Length of string: {length_of_string}")
        s = ""
        for _ in range(length_of_string):
            s += f.read(1).decode()
        return (s, f)


def rdb_file_process_metadata_section(f: BinaryIO) -> BinaryIO:
    key, f = rdb_file_process_string_encoded_value(f)
    print(f"Found metadata key: {key}")
    val, f = rdb_file_process_string_encoded_value(f)
    print(f"Found metadata value: {val}")
    print(f"Metadata key-value pair: {key} --> {val}")
    return f


def read_rdb_file_from_disk():
    with open(f"{args.dir}/{args.dbfilename}", "rb") as f:
        if f.read(9).decode() != "REDIS0011":
            raise ValueError("Malformed header")
        while True:
            curr_chunk = f.read(1)
            match curr_chunk:
                case b"\xFA":
                    print("Found metadata section")
                    f = rdb_file_process_metadata_section(f)
                case b"\xFE":
                    print("Found database section")
                    exit()


def handle_config_command(writer: asyncio.StreamWriter) -> None:
    get_command = decode_simple_string()
    if get_command != "GET":
        raise ValueError(
            f"'CONFIG' needs to be followed by 'GET'.\n" f"Instead, got {get_command}"
        )
    match decode_simple_string():
        case "dir":
            response = f"*2\r\n$3\r\ndir\r\n${len(args.dir)}\r\n{args.dir}\r\n"
            writer.write(response.encode())
        case "dbfilename":
            response = (
                f"*2\r\n$10\r\ndbfilename\r\n"
                f"${len(args.dbfilename)}\r\n{args.dbfilename}\r\n"
            )
            writer.write(response.encode())


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
            case "CONFIG":
                handle_config_command(writer)


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
    read_rdb_file_from_disk()
    asyncio.run(main())
