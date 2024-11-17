import asyncio
from collections import deque
import time
from typing import Optional, BinaryIO, cast
import argparse
import random
import string

# from enum import Enum


command_deque: deque[str] = deque()
# key_store: dict[str, tuple[str, Optional[float]]] = {}
# """key --> (value, expiry)"""
key_store: dict[str, "HashValue"] = {}

empty_rdb_file_hex = bytes.fromhex(
    "524544495330303131fa0972656469732d766572053"
    "72e322e30fa0a72656469732d62697473c040fa0563"
    "74696d65c26d08bc65fa08757365642d6d656dc2b0c"
    "41000fa08616f662d62617365c000fff06e3bfec0ff"
    "5aa2"
)


class HashValue:
    def __init__(self) -> None:
        pass

    def str_repr_of_val(self) -> str:
        return ""


class StringValue(HashValue):
    def __init__(self, str_val: str, expiry: Optional[float]) -> None:
        self.str_val = str_val
        self.expiry = expiry

    def str_repr_of_val(self) -> str:
        return f"${len(self.str_val)}\r\n{self.str_val}\r\n"


class StreamValue(HashValue):
    """
    Structure of entry in key_store with StreamEntry value
    ```
    key_store {
        stream_key: StreamEntry {
                                entry_id: {
                                    entry_key_1: entry_val_1,
                                    entry_key_2: entry_val_2,
                                    etc.
                    }
            }
    }
    ```
    """

    def __init__(self, entry_dict: dict) -> None:
        self.entry_dict = entry_dict


class NotEnoughBytesToProcessCommand(Exception):
    pass


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
bytes_received = 0


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
            key_store[key] = StringValue(str_val=val, expiry=None)
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
            key_store[key] = StringValue(str_val=val, expiry=expiry)
        # Indicates that this key ("baz") has an expire,
        # and that the expire timestamp is expressed in seconds. */
        case b"\xFD":
            expiry, f = rdb_file_process_expiry(f, 4)
            if f.read(1) != b"\x00":
                f.seek(-1, 1)
                raise ValueError(f"Invalid key_value type! (Found: {f.read(1)})")
            key, f = rdb_file_process_string_encoded_value(f)
            val, f = rdb_file_process_string_encoded_value(f)
            key_store[key] = StringValue(str_val=val, expiry=expiry)
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


def clear_bad_command(byte_ptr: int) -> int:
    for _ in range(byte_ptr):
        command_deque.popleft()
    while command_deque and command_deque[0] != "*":
        command_deque.popleft()
    return 0


def update_offset(byte_ptr: int) -> None:
    if IS_MASTER is False:
        global replica_offset
        replica_offset += byte_ptr
    else:
        global master_repl_offset
        master_repl_offset += byte_ptr


def xadd_auto_gen_seq_num(time: str, stream_key: str) -> str:
    if stream_key not in key_store and int(time) > 0:
        return "0"
    elif stream_key not in key_store:
        return "1"
    else:
        stream_value_for_given_stream_key = cast(StreamValue, key_store[stream_key])
        for k in reversed(stream_value_for_given_stream_key.entry_dict.keys()):
            curr_time, curr_seq_num = k.split("-")
            if time > curr_time:
                return "0"
            if time == curr_time:
                return str(int(curr_seq_num) + 1)
            if time < curr_time:
                continue
        return "1"


def xadd_auto_gen_time_and_seq_num() -> tuple[str, str]:

    return ("", "")


async def handle_xadd_command(
    writer: asyncio.StreamWriter, byte_ptr: int, command_length: int
) -> int:
    stream_key, byte_ptr = decode_bulk_string(byte_ptr)
    print(f"Stream key: {stream_key}")
    command_length -= 1
    entry_id, byte_ptr = decode_bulk_string(byte_ptr)
    command_length -= 1
    if entry_id == "*":
        time, sequenceNumber = xadd_auto_gen_time_and_seq_num()
    else:
        # Format of entry_id is <millisecondsTime>-<sequenceNumber>
        time, sequenceNumber = entry_id.split("-")
        if sequenceNumber == "*":
            sequenceNumber = xadd_auto_gen_seq_num(time, stream_key)
    print(f"Entry ID: {time}-{sequenceNumber}")
    if (command_length % 2) != 0:
        raise ValueError(
            "Supposed to be even number of items left in command "
            "(one key --> one value = one kv-pair)"
        )
    # *********************
    # * Create new stream *
    # *********************
    if stream_key not in key_store:
        entry_dict = {}
        num_of_kv_pairs_in_entry = int(command_length / 2)
        for _ in range(num_of_kv_pairs_in_entry):
            key, byte_ptr = decode_bulk_string(byte_ptr)
            val, byte_ptr = decode_bulk_string(byte_ptr)
            entry_dict[key] = val
        stream_entry = StreamValue(entry_dict={f"{time}-{sequenceNumber}": entry_dict})
        key_store[stream_key] = stream_entry
        writer.write(f"${len(entry_id)}\r\n{time}-{sequenceNumber}\r\n".encode())
        await writer.drain()
        return byte_ptr
    # ***************************
    # * Add to exisiting stream *
    # ***************************
    existing_entry = cast(StreamValue, key_store[stream_key])
    # The <millisecondsTime> is greater than or equal to the <millisecondsTime>
    # of the last entry
    last_entry_id_time, last_entry_id_seq_num = list(existing_entry.entry_dict.keys())[
        -1
    ].split("-")
    # If the stream is empty, the ID should be greater than 0-0
    if int(time) <= 0 and int(sequenceNumber) <= 1:
        writer.write(
            "-ERR The ID specified in XADD must be greater than 0-0\r\n".encode()
        )
        await writer.drain()
        print("-ERR The ID specified in XADD must be greater than 0-0")
        byte_ptr = clear_bad_command(byte_ptr)
        return byte_ptr
    if time < last_entry_id_time:
        writer.write(
            "-ERR The ID specified in XADD is equal or smaller than "
            "the target stream top item\r\n".encode()
        )
        await writer.drain()
        print(
            "-ERR The ID specified in XADD is equal or smaller "
            "than the target stream top item\r\n"
        )
        byte_ptr = clear_bad_command(byte_ptr)
        return byte_ptr
    # If the millisecondsTime part of the ID is equal to the millisecondsTime
    # of the last entry, the sequenceNumber part of the ID should be greater
    # than the sequenceNumber of the last entry
    if time == last_entry_id_time and sequenceNumber <= last_entry_id_seq_num:
        writer.write(
            "-ERR The ID specified in XADD is equal or smaller than "
            "the target stream top item\r\n".encode()
        )
        await writer.drain()
        print(
            "-ERR The ID specified in XADD is equal or smaller "
            "than the target stream top item\r\n"
        )
        byte_ptr = clear_bad_command(byte_ptr)
        return byte_ptr
    num_of_kv_pairs_in_entry = int(command_length / 2)
    # Process all entry k-v pairs into temp dict, then copy into existing entry dict
    temp_dict = {}
    for _ in range(num_of_kv_pairs_in_entry):
        key, byte_ptr = decode_bulk_string(byte_ptr)
        val, byte_ptr = decode_bulk_string(byte_ptr)
        temp_dict[key] = val
    new_entry_id = f"{time}-{sequenceNumber}"
    for k, v in temp_dict.items():
        if new_entry_id in existing_entry.entry_dict:
            existing_entry.entry_dict[new_entry_id][k] = v
        else:
            existing_entry.entry_dict[new_entry_id] = {k: v}

    writer.write(f"${len(entry_id)}\r\n{time}-{sequenceNumber}\r\n".encode())
    await writer.drain()
    return byte_ptr


async def handle_type_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    key, byte_ptr = decode_bulk_string(byte_ptr)
    if key not in key_store:
        writer.write("+none\r\n".encode())
        await writer.drain()
        return byte_ptr
    match key_store[key]:
        case StringValue():
            writer.write("+string\r\n".encode())
            await writer.drain()
        case StreamValue():
            writer.write("+stream\r\n".encode())
            await writer.drain()
        case _:
            raise TypeError(
                "Unable to handle type command of value associated "
                "with given key! "
                f"(Key: {key}\nValue: {repr(key_store[key])}"
            )
    return byte_ptr


async def handle_wait_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    print("Entered handle_wait_command")
    num_replicas, byte_ptr = decode_bulk_string(byte_ptr)
    timeout, byte_ptr = decode_bulk_string(byte_ptr)
    # await asyncio.sleep(int(timeout) / 1000)
    writer.write(f":{len(connected_replicas)}\r\n".encode())
    await writer.drain()
    return byte_ptr


async def handle_psync_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    s, byte_ptr = decode_bulk_string(byte_ptr)
    if s != "?":
        raise ValueError(
            "Expected `?` as first argument to `PSYNC` command. " f"Instead, got {s}"
        )
    s, byte_ptr = decode_bulk_string(byte_ptr)
    if s != "-1":
        raise ValueError(
            "Expected `-1` as second argument to `PSYNC` command. " f"Instead, got {s}"
        )
    writer.write(f"+FULLRESYNC {master_replid} 0\r\n".encode())
    await writer.drain()
    writer.write(f"${len(empty_rdb_file_hex)}\r\n".encode() + empty_rdb_file_hex)
    await writer.drain()

    return byte_ptr


async def handle_replconf_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    s, byte_ptr = decode_bulk_string(byte_ptr)
    print(f"matching s in replconf function: {s}")
    match s:
        case "listening-port":
            _, byte_ptr = decode_bulk_string(byte_ptr)
            if IS_MASTER:
                writer.write("+OK\r\n".encode())
                await writer.drain()
        case "capa":
            _, byte_ptr = decode_bulk_string(byte_ptr)
            if IS_MASTER:
                writer.write("+OK\r\n".encode())
                await writer.drain()
        case "GETACK":
            _, byte_ptr = decode_bulk_string(byte_ptr)
            resp = (
                f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_offset))}"
                f"\r\n{replica_offset}\r\n"
            )
            writer.write(resp.encode())
            await writer.drain()
        case _:
            raise ValueError("Unable to process `REPLCONF` command!")
    global connected_replicas
    if IS_MASTER:
        replica_conn_info = writer.get_extra_info("peername")
        connected_replicas[replica_conn_info] = writer

    return byte_ptr


async def handle_info_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    print(f"IS_MASTER = {IS_MASTER}")
    info_args, byte_ptr = decode_bulk_string(byte_ptr)
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
    await writer.drain()
    return byte_ptr


async def handle_keys_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    c, byte_ptr = decode_bulk_string(byte_ptr)
    if c != "*":
        raise ValueError(
            f"Can only handle '*' argument to keys command! " f"(Given: {c})"
        )
    s = f"*{len(key_store)}\r\n"
    for k in key_store:
        s += f"${len(k)}\r\n{k}\r\n"
    writer.write(s.encode())
    await writer.drain()
    return byte_ptr


async def handle_config_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    get_command, byte_ptr = decode_bulk_string(byte_ptr)
    if get_command != "GET":
        raise ValueError(
            f"'CONFIG' needs to be followed by 'GET'.\n" f"Instead, got {get_command}"
        )
    next_command, byte_ptr = decode_bulk_string(byte_ptr)
    match next_command:
        case "dir":
            response = f"*2\r\n$3\r\ndir\r\n${len(args.dir)}\r\n{args.dir}\r\n"
            writer.write(response.encode())
            await writer.drain()
            return byte_ptr
        case "dbfilename":
            response = (
                f"*2\r\n$10\r\ndbfilename\r\n"
                f"${len(args.dbfilename)}\r\n{args.dbfilename}\r\n"
            )
            writer.write(response.encode())
            await writer.drain()
            return byte_ptr
        case _:
            raise ValueError(
                f"Don't recognize argument to `CONFIG` command. (Given: {next_command})"
            )


async def handle_get_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    print("Entered 'handle_get_command' function")
    key, byte_ptr = decode_bulk_string(byte_ptr)
    if key not in key_store:
        writer.write("$-1\r\n".encode())
        await writer.drain()
        return byte_ptr
    entry = key_store[key]
    match entry:
        case StringValue():
            if entry.expiry is None:
                writer.write(entry.str_repr_of_val().encode())
                await writer.drain()
                return byte_ptr
            if time.time() > entry.expiry:
                writer.write("$-1\r\n".encode())
                await writer.drain()
                return byte_ptr
            writer.write(entry.str_repr_of_val().encode())
            await writer.drain()
            return byte_ptr
        case _:
            raise TypeError("Unable to process get command with given key")


async def handle_set_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    key, byte_ptr = decode_bulk_string(byte_ptr)
    val, byte_ptr = decode_bulk_string(byte_ptr)
    print(f"Decoded key-val: {key} --> {val}")
    print(f"byte_ptr after decoding key-val: {byte_ptr}")
    # Check for expiry
    try:
        possible_px, _ = decode_bulk_string(byte_ptr)
        if possible_px.lower() == "px":
            _, byte_ptr = decode_bulk_string(byte_ptr)
            exp, byte_ptr = decode_bulk_string(byte_ptr)
            expiry_length_seconds = float(exp) / 1000
            expiry_time = time.time() + expiry_length_seconds
        else:
            expiry_time = None
    except (NotEnoughBytesToProcessCommand, ValueError):
        expiry_time = None
    entry_val = StringValue(str_val=val, expiry=expiry_time)
    key_store[key] = entry_val
    print(f"Set {key} --> {repr(entry_val)}")
    if IS_MASTER:
        writer.write("+OK\r\n".encode())
        await writer.drain()
        global connected_replicas
        command_to_replicate = (
            f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
        )
        for replica_conn in connected_replicas.values():
            replica_conn.write(command_to_replicate.encode())

    return byte_ptr


async def handle_echo_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    print(f"Byte_ptr position when entering echo command: {byte_ptr}")
    c, byte_ptr = decode_bulk_string(byte_ptr)
    writer.write(f"${len(c)}\r\n{c}\r\n".encode())
    await writer.drain()
    return byte_ptr


async def handle_pong_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    if IS_MASTER:
        writer.write("+PONG\r\n".encode())
        await writer.drain()
    return byte_ptr


def decode_bulk_string(byte_ptr: int) -> tuple[str, int]:
    try:
        if command_deque[byte_ptr] != "$":
            raise ValueError(
                f"Expected '$' before length of bulk string. "
                f"Instead, got {command_deque[byte_ptr]}"
            )
        # Skip past "$"
        byte_ptr += 1
        str_len = ""
        while command_deque[byte_ptr] != "\r":
            str_len += command_deque[byte_ptr]
            byte_ptr += 1
        # Advanced two more to skip past newline
        byte_ptr += 2
        str_len = int(str_len)
        print(f"Length of bulk string to process = {str_len}")
        start = byte_ptr
        end = byte_ptr + str_len
        print(f"Bulk string start ind, end ind: {start}, {end}")
        result_str = "".join(command_deque[c] for c in range(start, end))
        # Skip past "\r\n" after end of string-contents
        byte_ptr = end + 2
        return (result_str, byte_ptr)
    except IndexError:
        raise NotEnoughBytesToProcessCommand("decode_bulk_string")


async def decode_array(writer: asyncio.StreamWriter) -> None:
    byte_ptr = 0
    while command_deque:
        if command_deque[byte_ptr] != "*":
            raise ValueError(
                f"Array is supposed to start with '*' "
                f"(Current byte_ptr = {byte_ptr}, "
                f"current char at byte_ptr = {command_deque[byte_ptr]}"
            )
        byte_ptr += 1
        # Get length of array
        arr_length = ""
        while command_deque[byte_ptr] != "\r":
            arr_length += command_deque[byte_ptr]
            byte_ptr += 1
        arr_length = int(arr_length)
        print(f"Length of array: {arr_length}")
        while byte_ptr < len(command_deque) and command_deque[byte_ptr] != "$":
            byte_ptr += 1
        print(f"byte_ptr before decoding command: {byte_ptr}")
        s, byte_ptr = decode_bulk_string(byte_ptr)
        print(f"Returned bulk string for decoding array: {s}")
        match s:
            case "PING":
                byte_ptr = await handle_pong_command(writer, byte_ptr)
            case "ECHO":
                byte_ptr = await handle_echo_command(writer, byte_ptr)
            case "SET":
                byte_ptr = await handle_set_command(writer, byte_ptr)
            case "GET":
                byte_ptr = await handle_get_command(writer, byte_ptr)
            case "CONFIG":
                byte_ptr = await handle_config_command(writer, byte_ptr)
            case "KEYS":
                byte_ptr = await handle_keys_command(writer, byte_ptr)
            case "INFO":
                byte_ptr = await handle_info_command(writer, byte_ptr)
            case "REPLCONF":
                byte_ptr = await handle_replconf_command(writer, byte_ptr)
            case "PSYNC":
                byte_ptr = await handle_psync_command(writer, byte_ptr)
            case "WAIT":
                byte_ptr = await handle_wait_command(writer, byte_ptr)
            case "TYPE":
                byte_ptr = await handle_type_command(writer, byte_ptr)
            case "XADD":
                # subtracting 1 from arr_length to account for 'XADD' bulk string
                # being processed already
                byte_ptr = await handle_xadd_command(writer, byte_ptr, arr_length - 1)
            case _:
                raise ValueError(f"Unrecognized command: {s}")
        for _ in range(byte_ptr):
            command_deque.popleft()
        update_offset(byte_ptr)
        byte_ptr = 0


async def connection_handler(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    while True:
        data = await reader.read(100)
        if not data:
            break
        print(f"Received {data}")
        for d in data.decode():
            command_deque.append(d)
        print(f"Updated command queue: {command_deque}")
        if command_deque:
            match command_deque[0]:
                case "*":
                    print("About to start decoding array")
                    try:
                        await decode_array(writer)
                    except NotEnoughBytesToProcessCommand as err:
                        print(
                            f"'{err.args}' did not have enough bytes to process command. "
                            "Now back in connection handler."
                        )
                case _:
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
    await writer.drain()
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
