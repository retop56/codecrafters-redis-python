#! /usr/bin/bash

printf "*1\r\n\$5\r\nMULTI\r\n" > tmp.txt
printf "*3\r\n\$3\r\nSET\r\n\$9\r\npineapple\r\n\$1\r\n9\r\n" >> tmp.txt
printf "*2\r\n\$4\r\nINCR\r\n\$9\r\npineapple\r\n" >> tmp.txt
printf "*2\r\n\$4\r\nINCR\r\n\$5\r\ngrape\r\n" >> tmp.txt
printf "*2\r\n$3\r\nGET\r\n$5\r\ngrape\r\n" >> tmp.txt
printf "*1\r\n\$4\r\nEXEC\r\n" >> tmp.txt

nc localhost 6379 < tmp.txt
