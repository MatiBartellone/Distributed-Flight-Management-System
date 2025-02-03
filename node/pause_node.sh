#!/usr/bin/env expect

set timeout 5

set container [lindex $argv 0]

spawn docker attach $container
send "pause\r"

expect eof