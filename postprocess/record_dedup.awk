BEGIN {
    prevuid = -1
    previid = -1
}
$1 != prevuid || $2 != previid{
    print $0
    prevuid = $1
    previid = $2
}