printf "*5\r\n\$4\r\nXADD\r\n\$6\r\norange\r\n\$3\r\n0-*\r\n\$5\r\nmango\r\n\$10\r\nstrawberry\r\n" > tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$6\r\norange\r\n\$3\r\n1-*\r\n\$5\r\nmango\r\n\$10\r\nstrawberry\r\n" >> tmp.txt
printf "*5\r\n\$4\r\nXADD\r\n\$6\r\norange\r\n\$3\r\n1-*\r\n\$10\r\nstrawberry\r\n\$9\r\npineapple\r\n" >> tmp.txt
nc localhost 6379 < tmp.txt
