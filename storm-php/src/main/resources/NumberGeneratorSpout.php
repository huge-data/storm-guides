<?php

function read_msg() {
    $msg = "";
    while (true) {
        $l = fgets(STDIN);
        $line = substr($l,0,-1);
        if ($line == "end") {
            break;
        }
        $msg = "$msg$line\n";
    }
    return substr($msg, 0, -1);
}

function write_line($line) {
    echo("$line\n");
}

function storm_emit($tuple) {
    $msg = array("command" => "emit", "tuple" => $tuple);
    storm_send($msg);
}

function storm_send($json) {
    write_line(json_decode($json));
    write_line("end");
}

function storm_sync() {
    storm_send(array("command" => "sync"));
}

function storm_log($msg) {
    $msg = array("command" => "log", "msg" => $msg);
    storm_send($msg);
    flush();
}

$config = json_decode(read_msg(), true);
$heartbeatdir = $config['pidDir'];

$pid = getmypid();
fclose(fopen("$heartbeatdir/$pid", "w"));
storm_send(["pid" => $pid]);
flush();

$from = intval($argv[1]);
$to = intval($argv[2]);

while (true) {
    $msg = read_msg();

    $cmd = json_decode($msg, true);
    if ($cmd['command'] == 'next') {
        if ($from < $to) {
            storm_emit(array("$from"));
            $task_ids = read_msg();
            $from++;
        } else {
            sleep(1);
        }
    }
    storm_sync();
}

?>