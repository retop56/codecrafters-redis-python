printf "*1\r\n\$5\r\nMULTI\r\n" > tmp.txt
printf "*3\r\n\$3\r\nSET\r\n\$5\r\nmango\r\n\$2\r\n69\r\n" >> tmp.txt
printf "*2\r\n\$4\r\nINCR\r\n\$5\r\nmango\r\n" >> tmp.txt

nc localhost 6379 < tmp.txt
