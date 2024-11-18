#! /usr/bin/bash

printf "*5\r\n\$4\r\nxadd\r\n\$4\r\npear\r\n\$3\r\n0-1\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n" > tmp.txt
printf "*5\r\n\$4\r\nxadd\r\n\$4\r\npear\r\n\$3\r\n0-2\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nxadd\r\n\$4\r\npear\r\n\$3\r\n0-3\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nxadd\r\n\$4\r\npear\r\n\$3\r\n0-4\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n" >> tmp.txt
printf "*4\r\n\$6\r\nxrange\r\n\$4\r\npear\r\n\$3\r\n0-2\r\n\$3\r\n0-4\r\n" >> tmp.txt

nc localhost 6379 < tmp.txt
