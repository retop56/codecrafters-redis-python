printf "*1\r\n\$4\r\nPING\r\n" > tmp.txt

nc localhost 6379 < tmp.txt
