import asyncio
from collections import deque
import time
from typing import Optional, BinaryIO, cast
import argparse
import random
import string
import re
import copy


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
    pass


class StringValue(HashValue):
    def __init__(self, str_val: str, expiry: Optional[float]) -> None:
        self.str_val = str_val
        self.expiry = expiry

    def str_repr_of_val(self) -> str:
        return f"${len(self.str_val)}\r\n{self.str_val}\r\n"


class StreamValue(HashValue):
    """
    Structure of entry in key_store with StreamValue value
    ```
    key_store {
        stream_key: StreamValue {
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

    def str_repr_of_val(self) -> str:
        repr_str = ""
        repr_str += f"*{len(self.entry_dict) * 2}\r\n"
        for k, v in self.entry_dict.items():
            repr_str += f"${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n"
        return repr_str


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


# async def xread_new_entries_only(
#     writer: asyncio.StreamWriter,
#     byte_ptr: int,
#     stream_keys: list,
#     block_time_ms: str,
# ) -> int:

#     # Capture current length of stream_key entries in key-store
#     for i, s in enumerate(stream_keys):
#         curr_stream_val = cast(StreamValue, key_store[s[0]])
#         stream_keys[i] = (s[0], len(curr_stream_val.entry_dict))
#     result_arr = []
#     # If <block_time_ms> is zero, wait until a change has occurred before responding
#     if block_time_ms == "0":
#         blocked_command_queue = copy.deepcopy(command_deque)
#         command_deque.clear()
#         while True:
#             for curr_key, prev_len in stream_keys:
#                 curr_key_entry_dict = cast(StreamValue, key_store[curr_key]).entry_dict
#                 if len(curr_key_entry_dict) > prev_len:
#                     break
#             else:
#                 await asyncio.sleep(0)
#                 continue
#             break
#         command_deque.extend(blocked_command_queue)
#         # Find entries that are greater than amount listed in stream_keys
#         for curr_key, prev_len in stream_keys:
#             curr_key_entry_dict = cast(StreamValue, key_store[curr_key]).entry_dict
#             if len(curr_key_entry_dict) > prev_len:
#                 pass
#             else:
#                 pass
#     # Block for <block_time_ms> amount of time and
#     else:
#         blocked_command_queue = copy.deepcopy(command_deque)
#         command_deque.clear()
#         await asyncio.sleep(int(block_time_ms) / 1000)
#         command_deque.extend(blocked_command_queue)

#     return byte_ptr


async def xread_w_blocking(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    regex_for_entry_id = re.compile(r"\d+-\d+")
    block_time_ms, byte_ptr = decode_bulk_string(byte_ptr)
    s, byte_ptr = decode_bulk_string(byte_ptr)
    if s != "streams":
        raise ValueError(
            f"block time should be followed by 'streams'. Instead, it's followed by {s}"
        )
    stream_keys_and_ids = []
    while True:
        possible_stream_key, byte_ptr = decode_bulk_string(byte_ptr)
        if (
            regex_for_entry_id.fullmatch(possible_stream_key)
            or possible_stream_key == "$"
        ):
            stream_keys_and_ids[0] = (*stream_keys_and_ids[0], possible_stream_key)
            break
        else:
            stream_keys_and_ids.append((possible_stream_key,))
    for i in range(1, len(stream_keys_and_ids)):
        entry_id, byte_ptr = decode_bulk_string(byte_ptr)
        stream_keys_and_ids[i] = (*stream_keys_and_ids[i], entry_id)
    if block_time_ms == "0":
        blocked_command_queue = copy.deepcopy(command_deque)
        command_deque.clear()
        # Check if stream_key even exists in key_store. If it doesn't, wait until
        # entry is created under stream name. If it does, wait until another entry
        # is entry is added under stream name.
        for i, v in enumerate(stream_keys_and_ids):
            stream_key, id = v
            if stream_key in key_store:
                curr_stream_val = cast(StreamValue, key_store[stream_key])
                curr_len = len(curr_stream_val.entry_dict)
                while True:
                    if len(curr_stream_val.entry_dict) > curr_len:
                        break
                    else:
                        await asyncio.sleep(0)
                if id == "$":
                    stream_keys_and_ids[i] = (stream_key, id, curr_len)
            else:
                while stream_key not in key_store:
                    await asyncio.sleep(0)
                if id == "$":
                    stream_keys_and_ids[i] = (stream_key, id, 0)
        command_deque.extend(blocked_command_queue)
    else:
        blocked_command_queue = copy.deepcopy(command_deque)
        command_deque.clear()
        print(f"Blocking for {block_time_ms} ms")
        await asyncio.sleep(int(block_time_ms) / 1000)
        command_deque.extend(blocked_command_queue)
    result_arr = []
    for e in stream_keys_and_ids:
        result_arr.append(xread_retrieve_entries(*e))
    result_str = f"*{len(stream_keys_and_ids)}\r\n" + "".join(s for s in result_arr)
    writer.write(result_str.encode())
    await writer.drain()
    return byte_ptr


def xread_retrieve_entries(
    stream_key: str, start_id: str, prev_len: Optional[int] = None
) -> str:
    if start_id != "$":
        gt_time, gt_seqNum = start_id.split("-")
        entries_for_stream_key = cast(StreamValue, key_store[stream_key]).entry_dict
        result_arr = []
        for k, v in entries_for_stream_key.items():
            curr_time, curr_seqNum = k.split("-")
            if curr_time >= gt_time and curr_seqNum > gt_seqNum:
                result_arr.append(encode_entry_to_array(k, v))
        if not result_arr:
            return "$-1\r\n"
        final_result = (
            f"*2\r\n${len(stream_key)}\r\n{stream_key}\r\n*{len(result_arr)}\r\n"
            + "".join(s for s in result_arr)
        )
        return final_result
    else:
        if prev_len is None:
            raise ValueError(
                "If start_id is '$', previous length of key should be provided!"
            )
        entries_for_stream_key = cast(StreamValue, key_store[stream_key]).entry_dict
        result_arr = []

        return ""


async def handle_xread_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    regex_for_entry_id = re.compile(r"\d+-\d+")
    s, byte_ptr = decode_bulk_string(byte_ptr)
    if s == "block":
        return await xread_w_blocking(writer, byte_ptr)
    if s != "streams":
        raise ValueError(
            f"'XREAD' should be followed by 'streams'. Instead, it's followed by {s}"
        )
    stream_keys_and_ids = []
    while True:
        possible_stream_key, byte_ptr = decode_bulk_string(byte_ptr)
        if regex_for_entry_id.fullmatch(possible_stream_key):
            stream_keys_and_ids[0] = (*stream_keys_and_ids[0], possible_stream_key)
            break
        else:
            stream_keys_and_ids.append((possible_stream_key,))
    for i in range(1, len(stream_keys_and_ids)):
        entry_id, byte_ptr = decode_bulk_string(byte_ptr)
        stream_keys_and_ids[i] = (*stream_keys_and_ids[i], entry_id)
    result_arr = []
    for e in stream_keys_and_ids:
        result_arr.append(xread_retrieve_entries(*e))
    result_str = f"*{len(stream_keys_and_ids)}\r\n" + "".join(s for s in result_arr)
    writer.write(result_str.encode())
    await writer.drain()
    return byte_ptr


def xrange_retrieve_entries_without_explicit_end(
    start_id: str, stream_key: str
) -> list[str]:
    if "-" in start_id:
        start_time, start_seqNum = start_id.split("-")
    else:
        start_time = start_id
        start_seqNum = None
    entries_for_stream_key = cast(StreamValue, key_store[stream_key]).entry_dict
    result_arr = []
    for k, v in entries_for_stream_key.items():
        curr_time, curr_seqNum = k.split("-")
        if start_time <= curr_time and (
            start_seqNum is None or start_seqNum <= curr_seqNum
        ):
            result_arr.append(encode_entry_to_array(k, v))
    return result_arr


def xrange_retrieve_entries_without_explicit_start(
    end_id: str, stream_key: str
) -> list[str]:
    if "-" in end_id:
        end_time, end_seqNum = end_id.split("-")
    else:
        end_time = end_id
        end_seqNum = None
    entries_for_stream_key = cast(StreamValue, key_store[stream_key]).entry_dict
    result_arr = []
    for k, v in entries_for_stream_key.items():
        curr_time, curr_seqNum = k.split("-")
        if end_time >= curr_time and (end_seqNum is None or end_seqNum >= curr_seqNum):
            result_arr.append(encode_entry_to_array(k, v))
    return result_arr


def xrange_retrieve_entries_with_explicit_start_and_stop(
    start_id: str, end_id: str, stream_key: str
) -> list[str]:
    if "-" in start_id:
        start_time, start_seqNum = start_id.split("-")
    else:
        start_time = start_id
        start_seqNum = None
    if "-" in end_id:
        end_time, end_seqNum = end_id.split("-")
    else:
        end_time = end_id
        end_seqNum = None
    entries_for_stream_key = cast(StreamValue, key_store[stream_key]).entry_dict
    result_arr = []
    for k, v in entries_for_stream_key.items():
        curr_time, curr_seqNum = k.split("-")
        # If entry id (k) we're currently looking at is in the range
        # of <start_id> - <end-id>, generate an array representation
        # of the entry_id and it's associated key-value pairs and
        # append to result_arr
        if (
            start_time <= curr_time
            and (start_seqNum is None or start_seqNum <= curr_seqNum)
            and end_time >= start_time
            and (end_seqNum is None or end_seqNum >= curr_seqNum)
        ):
            result_arr.append(encode_entry_to_array(k, v))
    return result_arr


def encode_entry_to_array(entry_id: str, val_dict: dict) -> str:
    result_str = f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n"
    result_str += f"*{len(val_dict)}\r\n"
    for k, v in val_dict.items():
        result_str += f"${len(k)}\r\n{k}\r\n"
        result_str += f"${len(v)}\r\n{v}\r\n"
    return result_str


async def handle_xrange_command(writer: asyncio.StreamWriter, byte_ptr: int) -> int:
    stream_key, byte_ptr = decode_bulk_string(byte_ptr)
    start_id, byte_ptr = decode_bulk_string(byte_ptr)
    end_id, byte_ptr = decode_bulk_string(byte_ptr)
    if start_id == "-":
        result_arr = xrange_retrieve_entries_without_explicit_start(end_id, stream_key)
    elif end_id == "+":
        result_arr = xrange_retrieve_entries_without_explicit_end(start_id, stream_key)
    else:
        result_arr = xrange_retrieve_entries_with_explicit_start_and_stop(
            start_id, end_id, stream_key
        )
    print("XRANGE result array (so far):")
    print(result_arr)
    final_result = f"*{len(result_arr)}\r\n" + "".join(r for r in result_arr)
    writer.write(final_result.encode())
    await writer.drain()
    return byte_ptr


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


def gen_time_and_SeqNum_from_entry_id(
    orig_entry_id: str, stream_key: str
) -> tuple[str, str]:
    if orig_entry_id == "*":
        time_id = str(int(time.time() * 1000))
        seqNum_id = "0"
        return (time_id, seqNum_id)
    # Format of entry_id is <millisecondsTime>-<sequenceNumber>
    time_id, seqNum_id = orig_entry_id.split("-")
    if seqNum_id == "*":
        if stream_key not in key_store and int(time_id) > 0:
            seqNum_id = "0"
        elif stream_key not in key_store:
            seqNum_id = "1"
        else:
            stream_value_for_given_stream_key = cast(StreamValue, key_store[stream_key])
            for k in reversed(stream_value_for_given_stream_key.entry_dict.keys()):
                curr_time, curr_seq_num = k.split("-")
                if time_id > curr_time:
                    seqNum_id = "0"
                    break
                if time_id == curr_time:
                    seqNum_id = str(int(curr_seq_num) + 1)
                    break
                if time_id < curr_time:
                    continue
            else:
                seqNum_id = "1"
    return (time_id, seqNum_id)


async def handle_xadd_command(
    writer: asyncio.StreamWriter, byte_ptr: int, command_length: int
) -> int:
    stream_key, byte_ptr = decode_bulk_string(byte_ptr)
    print(f"Stream key: {stream_key}")
    command_length -= 1
    orig_entry_id, byte_ptr = decode_bulk_string(byte_ptr)
    command_length -= 1
    time_id, seqNum_id = gen_time_and_SeqNum_from_entry_id(orig_entry_id, stream_key)
    new_entry_id = f"{time_id}-{seqNum_id}"
    print(f"Entry ID: {new_entry_id}")
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
        stream_entry = StreamValue(entry_dict={new_entry_id: entry_dict})
        key_store[stream_key] = stream_entry
        writer.write(f"${len(new_entry_id)}\r\n{new_entry_id}\r\n".encode())
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
    if int(time_id) <= 0 and int(seqNum_id) <= 1:
        writer.write(
            "-ERR The ID specified in XADD must be greater than 0-0\r\n".encode()
        )
        await writer.drain()
        print("-ERR The ID specified in XADD must be greater than 0-0")
        byte_ptr = clear_bad_command(byte_ptr)
        return byte_ptr
    if time_id < last_entry_id_time:
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
    if time_id == last_entry_id_time and seqNum_id <= last_entry_id_seq_num:
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
    for k, v in temp_dict.items():
        if new_entry_id in existing_entry.entry_dict:
            existing_entry.entry_dict[new_entry_id][k] = v
        else:
            existing_entry.entry_dict[new_entry_id] = {k: v}

    writer.write(f"${len(new_entry_id)}\r\n{new_entry_id}\r\n".encode())
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
    match s.lower():
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
        case "getack":
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
        byte_ptr += 1
        if command_deque[byte_ptr] != "\n":
            print(
                f"Expected \\n at index {byte_ptr}, instead found {command_deque[byte_ptr]}"
            )
            raise NotEnoughBytesToProcessCommand("decode_bulk_string")
        byte_ptr += 1
        str_len = int(str_len)
        print(f"Length of bulk string to process = {str_len}")
        start = byte_ptr
        end = byte_ptr + str_len
        print(f"Bulk string start ind, end ind: {start}, {end}")
        result_str = "".join(command_deque[c] for c in range(start, end))
        # Skip past "\r\n" after end of string-contents
        end_of_bulk_string = f"{command_deque[end]}{command_deque[end + 1]}"
        if end_of_bulk_string != "\r\n":
            print(
                f"Expected \\r\\n at end of bulk string, instead got {end_of_bulk_string}"
            )
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
        match s.lower():
            case "ping":
                byte_ptr = await handle_pong_command(writer, byte_ptr)
            case "echo":
                byte_ptr = await handle_echo_command(writer, byte_ptr)
            case "set":
                byte_ptr = await handle_set_command(writer, byte_ptr)
            case "get":
                byte_ptr = await handle_get_command(writer, byte_ptr)
            case "config":
                byte_ptr = await handle_config_command(writer, byte_ptr)
            case "keys":
                byte_ptr = await handle_keys_command(writer, byte_ptr)
            case "info":
                byte_ptr = await handle_info_command(writer, byte_ptr)
            case "replconf":
                byte_ptr = await handle_replconf_command(writer, byte_ptr)
            case "psync":
                byte_ptr = await handle_psync_command(writer, byte_ptr)
            case "wait":
                byte_ptr = await handle_wait_command(writer, byte_ptr)
            case "type":
                byte_ptr = await handle_type_command(writer, byte_ptr)
            case "xadd":
                # subtracting 1 from arr_length to account for 'XADD' bulk string
                # being processed already
                byte_ptr = await handle_xadd_command(writer, byte_ptr, arr_length - 1)
            case "xrange":
                byte_ptr = await handle_xrange_command(writer, byte_ptr)
            case "xread":
                byte_ptr = await handle_xread_command(writer, byte_ptr)
            case _:
                raise ValueError(f"Unrecognized command: {s}")
        for _ in range(byte_ptr):
            if not command_deque:
                break
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
