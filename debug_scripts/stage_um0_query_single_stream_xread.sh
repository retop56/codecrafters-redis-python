#! /usr/bin/bash

printf "*5\r\n\$4\r\nxadd\r\n\$9\r\nraspberry\r\n\$3\r\n0-1\r\n\$11\r\ntemperature\r\n\$2\r\n75\r\n" > tmp.txt
printf "*4\r\n\$5\r\nxread\r\n\$7\r\nstreams\r\n\$9\r\nraspberry\r\n\$3\r\n0-0\r\n" >> tmp.txt

nc localhost 6379 < tmp.txt


