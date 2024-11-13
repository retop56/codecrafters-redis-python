import asyncio
from collections import deque
import time
from typing import Optional, BinaryIO
import argparse
import random
import string

command_queue: deque[str] = deque()
key_store: dict[str, tuple[str, Optional[float]]] = {}
"""key --> (value, expiry)"""
empty_rdb_file_hex = bytes.fromhex(
    "524544495330303131fa0972656469732d766572053"
    "72e322e30fa0a72656469732d62697473c040fa0563"
    "74696d65c26d08bc65fa08757365642d6d656dc2b0c"
    "41000fa08616f662d62617365c000fff06e3bfec0ff"
    "5aa2"
)


parser = argparse.ArgumentParser(
    prog="Bootleg Redis",
    description="'Build your own Redis' Codecrafters challenge",
)
parser.add_argument("--dir")
parser.add_argument("--dbfilename")
parser.add_argument("--port", type=int, default=6379)
parser.add_argument("--replicaof")
args = parser.parse_args()

if args.replicaof:
    IS_MASTER = False
    replica_offset = 0
else:
    IS_MASTER = True
    connected_replicas: dict[tuple[str, int], asyncio.StreamWriter] = {}
    master_replid = "".join(
        random.choices([*string.ascii_lowercase, *string.digits], k=40)
    )
    master_repl_offset = 0
temp_bytes_processed = 0


def rdb_file_process_expiry(f: BinaryIO, bytes_to_read: int) -> tuple[float, BinaryIO]:
    if bytes_to_read == 4:
        expire_seconds = int.from_bytes(f.read(bytes_to_read), byteorder="little")
        return (expire_seconds, f)
    elif bytes_to_read == 8:
        expire_ms = int.from_bytes(f.read(bytes_to_read), byteorder="little") / 1000
        return (expire_ms, f)
    else:
        raise ValueError("Unable to process expiry time for key_value read from file!")


def rdb_file_process_ht_entry(f: BinaryIO) -> BinaryIO:
    first_byte = f.read(1)
    match first_byte:
        # The 1-byte flag that specifies the value’s type and encoding.
        # Here, the flag is 0, which means "string.
        case b"\x00":
            key, f = rdb_file_process_string_encoded_value(f)
            val, f = rdb_file_process_string_encoded_value(f)
            key_store[key] = (val, None)
        # Indicates that this key has an expire, and that the expire
        # timestamp is expressed in milliseconds
        case b"\xFC":
            # The expire timestamp, expressed in Unix time,
            # stored as an 8-byte unsigned long, in little-endian (read right-to-left)
            expiry, f = rdb_file_process_expiry(f, 8)
            if f.read(1) != b"\x00":
                f.seek(-1, 1)
                raise ValueError(f"Invalid key_value type! (Found: {f.read(1)})")
            key, f = rdb_file_process_string_encoded_value(f)
            val, f = rdb_file_process_string_encoded_value(f)
            key_store[key] = (val, expiry)
        # Indicates that this key ("baz") has an expire,
        # and that the expire timestamp is expressed in seconds. */
        case b"\xFD":
            expiry, f = rdb_file_process_expiry(f, 4)
            if f.read(1) != b"\x00":
                f.seek(-1, 1)
                raise ValueError(f"Invalid key_value type! (Found: {f.read(1)})")
            key, f = rdb_file_process_string_encoded_value(f)
            val, f = rdb_file_process_string_encoded_value(f)
            key_store[key] = (val, expiry)
        case _:
            raise ValueError("Unable to process hash-table entry from RDB file!")

    return f


def rdb_file_process_database_section(f: BinaryIO) -> BinaryIO:
    # Get index of database (no current known use)
    _, f = rdb_file_process_size_encoded_value(f)
    if f.read(1) != b"\xFB":
        f.seek(-1, 1)
        raise ValueError(f'Expected b"\xFB", instead got {f.read(1)}')
    ht_total_keys, f = rdb_file_process_size_encoded_value(f)
    ht_expiry_keys, f = rdb_file_process_size_encoded_value(f)
    print(f"Total keys: {ht_total_keys}")
    print(f"Keys with expiry: {ht_expiry_keys}")
    for _ in range(ht_total_keys):
        f = rdb_file_process_ht_entry(f)
    return f


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
    val, f = rdb_file_process_string_encoded_value(f)
    print(f"Metadata key-value pair: {key} --> {val}")
    return f


def read_rdb_file_from_disk():
    try:
        with open(f"{args.dir}/{args.dbfilename}", "rb") as f:
            if f.read(9).decode() != "REDIS0011":
                raise ValueError("Malformed header")
            while True:
                curr_chunk = f.read(1)
                match curr_chunk:
                    case b"\xFA":
                        f = rdb_file_process_metadata_section(f)
                    case b"\xFE":
                        f = rdb_file_process_database_section(f)
                    case _:
                        break
            print(f"Keystore after reading from file: {key_store}")
    except FileNotFoundError:
        return


def handle_psync_command(writer: asyncio.StreamWriter) -> None:
    s = decode_bulk_string()
    if s != "?":
        raise ValueError(
            "Expected `?` as first argument to `PSYNC` command. " f"Instead, got {s}"
        )
    s = decode_bulk_string()
    if s != "-1":
        raise ValueError(
            "Expected `-1` as second argument to `PSYNC` command. " f"Instead, got {s}"
        )
    writer.write(f"+FULLRESYNC {master_replid} 0\r\n".encode())
    writer.write(f"${len(empty_rdb_file_hex)}\r\n".encode() + empty_rdb_file_hex)


def handle_replconf_command(writer: asyncio.StreamWriter) -> None:
    s = decode_bulk_string()
    print(f"matching s in replconf function: {s}")
    match s:
        case "listening-port":
            decode_bulk_string()
            if IS_MASTER:
                writer.write("+OK\r\n".encode())
        case "capa":
            decode_bulk_string()
            if IS_MASTER:
                writer.write("+OK\r\n".encode())
        case "GETACK":
            decode_bulk_string()
            resp = (
                f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_offset))}"
                f"\r\n{replica_offset}\r\n"
            )
            writer.write(resp.encode())
        case _:
            raise ValueError("Unable to process `REPLCONF` command!")
    global connected_replicas
    if IS_MASTER:
        replica_conn_info = writer.get_extra_info("peername")
        connected_replicas[replica_conn_info] = writer


def handle_info_command(writer: asyncio.StreamWriter) -> None:
    print(f"IS_MASTER = {IS_MASTER}")
    info_args = decode_bulk_string()
    if info_args != "replication":
        raise ValueError("Invalid argument to `INFO` command! " f"(Given: {info_args})")
    s = ""
    if IS_MASTER:
        s += "role:master"
        s += f"\nmaster_replid:{master_replid}"
        s += f"\nmaster_repl_offset:{master_repl_offset}"
    else:
        s += "role:slave"
    s = f"${len(s)}\r\n{s}\r\n"
    writer.write(s.encode())


def handle_keys_command(writer: asyncio.StreamWriter) -> None:
    c = decode_bulk_string()
    if c != "*":
        raise ValueError(
            f"Can only handle '*' argument to keys command! " f"(Given: {c})"
        )
    s = f"*{len(key_store)}\r\n"
    for k in key_store:
        s += f"${len(k)}\r\n{k}\r\n"
    writer.write(s.encode())


def handle_config_command(writer: asyncio.StreamWriter) -> None:
    get_command = decode_bulk_string()
    if get_command != "GET":
        raise ValueError(
            f"'CONFIG' needs to be followed by 'GET'.\n" f"Instead, got {get_command}"
        )
    match decode_bulk_string():
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
    print("Entered 'handle_get_command' function")
    key = decode_bulk_string()
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
    key = decode_bulk_string()
    val = decode_bulk_string()
    print(f"Decoded key-val: {key} --> {val}")
    print(f"Command queue after decoding key-val: {command_queue}")
    if len(command_queue) > 1 and command_queue[1].lower() == "px":
        decode_bulk_string()
        expiry_length_seconds = float(decode_bulk_string()) / 1000
        expiry_time = time.time() + expiry_length_seconds
    else:
        expiry_time = None
    key_store[key] = (val, expiry_time)
    print(f"Set {key} --> {(val, expiry_time)}")
    if IS_MASTER:
        writer.write("+OK\r\n".encode())
        global connected_replicas
        command_to_replicate = (
            f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
        )
        for replica_conn in connected_replicas.values():
            replica_conn.write(command_to_replicate.encode())


def handle_echo_command(writer: asyncio.StreamWriter) -> None:
    c = decode_bulk_string()
    writer.write(f"${len(c)}\r\n{c}\r\n".encode())


def decode_bulk_string() -> str:
    # Adding length of command + 2 for '\r\n' that comes afterwards but is missing because of
    # split before adding to command_queue
    # Remove $<length-of-string>
    if command_queue.popleft().startswith("$") is False:
        raise ValueError("Simple String must be preceded by '$<length-of-string>'")
    return command_queue.popleft()


def decode_array(writer: asyncio.StreamWriter) -> None:
    # Turn "*<number>" into integer, then check whether we have that number of elements
    # in command queue. If not, throw it back to connection handler to continue
    # reading in bytes over connection into queue
    while command_queue:
        expected_queue_length = int(command_queue.popleft()[1:]) * 2
        if expected_queue_length > len(command_queue):
            print(
                f"Expected queue length of {expected_queue_length}, "
                f"actual queue length is {len(command_queue)}"
            )
            return

        if command_queue[0].startswith("$"):
            s = decode_bulk_string()
            match s:
                case "PING":
                    if IS_MASTER:
                        writer.write("+PONG\r\n".encode())
                case "ECHO":
                    handle_echo_command(writer)
                case "SET":
                    handle_set_command(writer)
                case "GET":
                    handle_get_command(writer)
                case "CONFIG":
                    handle_config_command(writer)
                case "KEYS":
                    handle_keys_command(writer)
                case "INFO":
                    handle_info_command(writer)
                case "REPLCONF":
                    handle_replconf_command(writer)
                case "PSYNC":
                    handle_psync_command(writer)
                case _:
                    raise ValueError(f"Unrecognized command: {s}")


async def connection_handler(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    while True:
        data = await reader.read(100)
        if not data:
            break
        print(f"Received {data}")
        # Remove any empty strings from message after splitting
        data = [chunk for chunk in data.decode().split("\r\n") if chunk]
        for d in data:
            command_queue.append(d)
        print(f"Updated command queue: {command_queue}")
        # Checking first character of first item in command queue
        if command_queue:
            match command_queue[0][0]:
                case "*":
                    print("About to start decoding array")
                    decode_array(writer)
                    await writer.drain()
                case _:
                    # await asyncio.sleep(0)
                    break


async def skip_past_rdb_file_sent_over_wire(replica_reader: asyncio.StreamReader):
    data = await replica_reader.read(1)
    if data.decode() != "$":
        raise ValueError(
            "Expected '$' before length of rdb_file sent by Master. "
            f"Instead, got '{data.decode()}'"
        )
    length_of_rdb_file = ""
    while True:
        b = await replica_reader.read(1)
        if b.decode() == "\r":
            break
        length_of_rdb_file += b.decode()
    # Adding 1 to skip past '\n' before rdb_file data starts
    await replica_reader.read(int(length_of_rdb_file) + 1)


async def replica_handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    print("Replica handshake start")
    writer.write("*1\r\n$4\r\nPING\r\n".encode())
    await writer.drain()
    data = await reader.read(1024)
    if data.decode() != "+PONG\r\n":
        raise ValueError(
            "Expected '+PONG\\r\\n' in response to 'PING'. "
            f"Instead, got {data.decode()}"
        )
    writer.write(
        f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{args.port}\r\n".encode()
    )
    await writer.drain()
    data = await reader.read(1024)
    if data.decode() != "+OK\r\n":
        raise ValueError(
            "Expected '+OK\\r\\n' in response to first 'REPLCONF'. "
            f"Instead, got {data.decode()}"
        )
    writer.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
    await writer.drain()
    data = await reader.read(1024)
    if data.decode() != "+OK\r\n":
        raise ValueError(
            "Expected '+OK\\r\\n' in response to second 'REPLCONF'. "
            f"Instead, got {data.decode()}"
        )
    writer.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())
    psync_response = await reader.read(56)
    print(f"psync_response = {psync_response}")


async def replica_start() -> None:
    HOST, PORT = args.replicaof.split()
    replica_reader, replica_writer = await asyncio.open_connection(HOST, PORT)
    await replica_handshake(replica_reader, replica_writer)
    print("Replica handshake done")
    await skip_past_rdb_file_sent_over_wire(replica_reader)
    server_socket = await asyncio.start_server(
        connection_handler, "localhost", args.port
    )
    await connection_handler(replica_reader, replica_writer)
    print("Right after connection handler started for replica")
    async with server_socket:
        print("Right before serve_forever")
        await server_socket.serve_forever()


async def master_start():
    server_socket = await asyncio.start_server(
        connection_handler, "localhost", args.port
    )
    async with server_socket:
        await server_socket.serve_forever()


if __name__ == "__main__":
    if IS_MASTER:
        read_rdb_file_from_disk()
        asyncio.run(master_start())
    else:
        asyncio.run(replica_start())
