printf "*1\r\n\$4\r\nPING\r\n*3\r\n\$8\r\nREPLCONF\r\n\$6\r\nGETACK\r\n\$1\r\n*\r\n" | nc localhost 6379
