#!/usr/bin/env expect

set container [lindex $argv 0]
exec ./send_command.sh $container "resume"