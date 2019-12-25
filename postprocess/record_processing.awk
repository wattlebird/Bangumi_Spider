NR != 0 && $1 != prev {
    p = 0 
    for (i = 1; i <= 10; i++) {
        if (stat[i] != 0) {
            p += stat[i]
            stat[i] = p/count
        }
    }
    for (itm in record) {
        printf("%d\t%d\t%d\t%.6f\n", prev, itm, record[itm], stat[record[itm]])
    }
    delete record
    delete stat
    count = 0
    prev = $1
}
{
    count += 1
    stat[$3] += 1
    record[$2] = $3
}
END {
    p = 0 
    for (i = 1; i <= 10; i++) {
        if (stat[i] != 0) {
            p += stat[i]
            stat[i] = p/count
        }
    }
    for (itm in record) {
        printf("%d\t%d\t%d\t%.6f\n", prev, itm, record[itm], stat[record[itm]])
    }
}
