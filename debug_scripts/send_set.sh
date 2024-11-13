#! /usr/bin/bash

printf "*3\r\n\$3\r\nSET\r\n\$6\r\norange\r\n\$9\r\npineapple\r\n" | nc localhost 6379
