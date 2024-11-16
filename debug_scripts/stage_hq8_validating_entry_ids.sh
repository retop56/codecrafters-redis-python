#! /usr/bin/bash

printf "*5\r\n\$4\r\nXADD\r\n\$6\r\nbanana\r\n\$3\r\n1-1\r\n\$10\r\nstrawberry\r\n\$4\r\npear\r\n" > tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$6\r\nbanana\r\n\$3\r\n1-2\r\n\$5\r\napple\r\n\$9\r\nraspberry\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$6\r\nbanana\r\n\$3\r\n1-2\r\n\$9\r\npineapple\r\n\$9\r\nblueberry\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$6\r\nbanana\r\n\$3\r\n0-3\r\n\$5\r\ngrape\r\n\$5\r\nmango\r\n" >> tmp.txt
nc localhost 6379 < tmp.txt
