#! /usr/bin/bash

printf "*5\r\n\$4\r\nXADD\r\n\$4\r\npear\r\n\$3\r\n0-1\r\n\$5\r\napple\r\n\$6\r\norange\r\n" > tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$4\r\npear\r\n\$3\r\n0-2\r\n\$10\r\nstrawberry\r\n\$5\r\napple\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$4\r\npear\r\n\$3\r\n0-3\r\n\$5\r\nmango\r\n\$6\r\nbanana\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$4\r\npear\r\n\$3\r\n0-4\r\n\$5\r\nmango\r\n\$6\r\norange\r\n" >> tmp.txt
printf "*4\r\n\$6\r\nXRANGE\r\n\$4\r\npear\r\n\$3\r\n0-2\r\n\$3\r\n0-4\r\n" >> tmp.txt

nc localhost 6379 < tmp.txt

