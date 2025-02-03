#!/usr/bin/env expect

set timeout 5

set container [lindex $argv 0]
set command [lrange $argv 1 end]

spawn docker attach $container
send "$command\r"

expect eof
