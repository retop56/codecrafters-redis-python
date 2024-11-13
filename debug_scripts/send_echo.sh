#! usr/bin/bash
echo -n $redis_ping_string | xxd -r -p | nc localhost 6379
