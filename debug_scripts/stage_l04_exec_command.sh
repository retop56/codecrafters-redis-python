#! /usr/bin/bash

printf "*1\r\n\$4\r\nEXEC\r\n" > tmp.txt

nc localhost 6379 < tmp.txt
