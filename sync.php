<?php
echo "<pre>";
$schema      = 'public';
$table       = 'daya_tampung';
$host        = "host = localhost";
$port        = "port = 5432";
$dbname      = "dbname = feeder";
$credentials = "user = postgres password=root";
$url         = '192.168.4.28:8082/syncserver/server.php?q=push_data_server';

$count_per_batch = 30;
$primary_key_1 = 'id_smt';
$primary_key_2 = 'id_sms';
$primaries = [$primary_key_1, $primary_key_2];

$conn = pg_connect("$host $port $dbname $credentials");
if (!$conn) {
    echo "Error : Unable to open database\n";
    exit;
}

$sql_columns = "SELECT column_name FROM information_schema.columns WHERE table_schema = '$schema' AND table_name = '$table';";
$res = pg_query($conn, $sql_columns);
while ($row = pg_fetch_row($res)) {
    $structures[] = $row[0];
}

$sql_data = "SELECT * FROM $table WHERE last_update > last_sync;";
$res = pg_query($conn, $sql_data);
while ($row = pg_fetch_row($res)) {
    $rows[] = $row;
}

$length = count($rows);
$batch = ceil($length / $count_per_batch);

$post = [
    'schema' => $schema,
    'table' => $table,
    'primary_keys' => $primaries,
    'struktur' => $structures
];

echo "---------Syncronization Start----------<br/>";

if (empty($rows)) {
    echo "-------------No Data Sent--------------<br/>";
}

for ($i = 1; $i <= $batch; $i++) {
    $updated = 0;
    $rows_send = [];
    for ($j = $count_per_batch * ($i - 1); $j < $count_per_batch * $i; $j++) {
        if (!empty($rows[$j])) {
            $rows_send[] = $rows[$j];
        }
    }

    $post['rows'] = $rows_send;
    $req = base64_encode(json_encode($post));
    $fields = ['p' => $req];
    $fields_string = http_build_query($fields);
    $ch = curl_init();

    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $fields_string);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

    $result = curl_exec($ch);
    $encoded_result = json_decode(base64_decode($result));

    curl_close($ch);

    if (!empty($encoded_result->error_desc)) {
        echo "Error $encoded_result->error_code : $encoded_result->error_desc";
    }

    foreach ($rows_send as $row) {
        $res = pg_update($conn, $table, ['last_sync' => date("Y-m-d h:i:s")], [$primary_key_1 => $row[0], $primary_key_2 => $row[1]]);
        if ($res) {
            $updated += 1;
        }
    }

    echo "Success : " . $encoded_result->data->success->total . ", Failed : " . $encoded_result->data->failed->total . "<br/>";
    echo "Update last_sync on " . $updated . " data <br/>";
    echo "--------------------------------------<br/>";
}

echo "---------Syncronization Finish----------<br/>";


?>
<br>
<a href="index.php">
    <button>Back to Home</button>
</a>