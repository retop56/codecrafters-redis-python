printf "*5\r\n\$4\r\nxadd\r\n\$9\r\nraspberry\r\n\$3\r\n0-1\r\n\$11\r\ntemperature\r\n\$2\r\n15\r\n" > tmp.txt
printf "*6\r\n\$5\r\nxread\r\n\$5\r\nblock\r\n\$1\r\n0\r\n\$7\r\nstreams\r\n\$9\r\nraspberry\r\n\$1\r\n\$\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nxadd\r\n\$9\r\nraspberry\r\n\$3\r\n0-2\r\n\$11\r\ntemperature\r\n\$2\r\n15\r\n" >> tmp.txt
nc localhost 6379 < tmp.txt
