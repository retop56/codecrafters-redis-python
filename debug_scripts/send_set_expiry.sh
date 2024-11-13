#! /usr/bin/bash
printf "*5\r\n\$3\r\nSET\r\n\$9\r\nraspberry\r\n\$10\r\nstrawberry\r\n\$2\r\npx\r\n\$3\r\n100\r\n" | nc localhost 6379
