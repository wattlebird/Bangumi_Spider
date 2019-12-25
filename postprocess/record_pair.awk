# assumes subject.tsv and three columns in input file: uid, iid, score
BEGIN {
    while(getline < "subject.tsv" != 0) {
        subjects[$1] = 1
    }
}
NR != 0 && $1 != prev {
    if (len >= 2) {
        for (i=1; i<=len; i++) {
            for (j=i+1; j<=len; j++) {
                host[iid[i]"_"iid[j]] += src[i]
                visit[iid[i]"_"iid[j]] += src[j]
                count[iid[i]"_"iid[j]] += 1
            }
        }
        delete iid
        delete src
        len = 0
    }
}
{
    len += 1
    if ($2 in subjects) {
        iid[len] = $2
        src[len] = $3
    }
    prev = $1
}
END {
    if (len >= 2) {
        for (i=1; i<=len; i++) {
            for (j=i+1; j<=len; j++) {
                host[iid[i]"_"iid[j]] += src[i]
                visit[iid[i]"_"iid[j]] += src[j]
                count[iid[i]"_"iid[j]] += 1
            }
        }
        delete iid
        delete src
        len = 0
    }
    for(p in count) {
        split(p, arr, "_")
        printf("%d\t%d\t%.6f\t%.6f\n", arr[1], arr[2], host[p]/count[p], visit[p]/count[p])
    }
}