#! /usr/bin/bash

printf "*5\r\n\$4\r\nXADD\r\n\$5\r\nmango\r\n\$3\r\n1-1\r\n\$6\r\norange\r\n\$4\r\npear\r\n" > send_multiple_xadd.txt
printf "*5\r\n\$4\r\nXADD\r\n\$5\r\nmango\r\n\$3\r\n1-2\r\n\$5\r\nmango\r\n\$5\r\ngrape\r\n" >> send_multiple_xadd.txt
printf "*5\r\n\$4\r\nXADD\r\n\$5\r\nmango\r\n\$3\r\n1-2\r\n\$10\r\nstrawberry\r\n\$9\r\npineapple\r\n" >> send_multiple_xadd.txt
nc localhost 6379 < send_multiple_xadd.txt
