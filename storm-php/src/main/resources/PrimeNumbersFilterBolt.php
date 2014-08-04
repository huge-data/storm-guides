<?php

function isPrime($number) {
    if ($number < 2) {
        return false;
    }
    if ($number == 2) {
        return true;
    }
    for ($i = 2; $i <= $number; $i++) {
        if ($number % $i == 0) {
            return false;
        }
    }
    return true;
}

function read_msg() {
    $msg = "";
    while (true) {
        $l = fgets(STDIN);
        $line = substr($l, 0, -1);
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

function storm_ack($id) {
    storm_send(["commans" => "ack", "id" => "$id"]);
}

function storm_log($msg) {
    $msg = array("command" => "log", "msg" => "$msg");
    storm_send($msg);
}

$config = json_decode(read_msg(), true);
$heartbeatdir = $config['pidDir'];

$pid = getmypid();
fclose(fopen("$heartbeatdir/$pid", "w"));
storm_send(["pid" => $pid]);
flush();

while (true) {
    $msg = read_msg();
    $tuple = json_decode($msg, true, 512, JSON_BIGINT_AS_STRING);
    if (!empty($tuple['id'])) {
        if (isPrime($tuple["tuple"][0])) {
            storm_emit(array($tuple["tuple"][0]));
        }
        storm_ack($tuple["id"]);
    }
}


?>