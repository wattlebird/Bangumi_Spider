#!/bin/bash
# fetch record and subject file from azure storage service

echo "Begining fetching filename..."

recordfile=$(az storage blob list -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -o table --prefix record | sed 1,2d | tr -s '\040' '\011' | cut -f1,6 | sort -t$'\t' -k2,2r | sed 1q | sed 's/\t.*//g')
echo "Record file read as ${recordfile}."

subjectfile=$(az storage blob list -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -o table --prefix subject | sed 1,2d | grep 'subject\-api\-[0-9]\+' | tr -s '\040' '\011' | cut -f1,6 | sort -t$'\t' -k2,2r | sed 1q | sed 's/\t.*//g')
echo "API scraped subject file read as ${subjectfile}."


az storage blob download -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -n "$recordfile" -f "$recordfile" && echo "Record file downloaded under ${PWD}."
az storage blob download -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -n "$subjectfile" -f "$subjectfile" && echo "Subject file downloaded under ${PWD}."
aujourdhui=$(date +"%Y_%m_%d")

# Preprocess files to remove scrapy error
echo "Preprocessing record and subject files."
sed -i 's~\r~~g' $recordfile
sed -i 's~\r~~g' $subjectfile

echo "Sorting files."
sed 1d $subjectfile | sort -t$'\t' -k2,2n > subject.sorted
echo "Solving redirected subjects..."
sed 1d $recordfile | sort -t$'\t' -k2,2 > record.right
cut -f1,2 subject.sorted | sort -t$'\t' -k2,2 > subject.left
# Join the record with subject order to get real subject id of each record's iid
# Drop the duplicated <uid, iid> caused by subject-redirection or re-scrape
join -12 -22 -t$'\t' -o 2.1,1.1,2.3,2.4,2.5,2.6,2.7,2.8 subject.left record.right | sort -t$'\t' -k1,1n -k2,2n | gawk -F "\t" -f record_dedup.awk > record_"$aujourdhui".tsv
# Remove duplicated subject in subject.tsv
awk -F "\t" '$1==$2 {print $0;}' < subject.sorted | cut -f2 --complement | sort -t$'\t' -k1,1 > subject_"$aujourdhui".tsv
echo "Cleaning up..."
rm subject.left subject.sorted record.right

# publish?
echo "Publish data."
az storage file upload --share-name bangumi-publish/subject --source subject_"$aujourdhui".tsv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
az storage file upload --share-name bangumi-publish/record --source record_"$aujourdhui".tsv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT

# 1. filter out ranked anime in record
echo "Make sure only anime is considered."
gawk -F "\t" '$3=="anime" && length($6)!=0 {printf("%d\t%d\t%d\n", $1, $2, $6)}' record_"$aujourdhui".tsv > record.tsv
# 2. calculate cdf for each user
echo "Doing some preprocessing tasks, like normalizing"
sort -t$'\t' -k1,1n -k3,3n record.tsv > record.sorted.tsv
gawk -f record_processing.awk -F "\t" record.sorted.tsv | sort -t$'\t' -k1,1n -k2,2n > record.anime.tsv
awk -F "\t" '$4=="anime" && length($5)!=0 {printf("%d\t%s\t%d\n", $1, $2, $5)}' subject_"$aujourdhui".tsv > subject.tsv
# 3. generate averaged score pair on avg, cdf and prob normalization methods.
echo "Generating paired scores."
cut -f1,2,3 record.anime.tsv | awk -F "\t" -f record_pair.awk > pair.avg.tsv
cut -f1,2,4 record.anime.tsv | awk -F "\t" -f record_pair.awk > pair.cdf.tsv
cut -f1,2,3 record.anime.tsv | awk -F "\t" -f record_pair_prob.spec.awk > pair.prob.tsv
# 4. calculate custom rank using rankit.
echo "Calculating rank."
python customrank.py
# 5. upload the custom rank to Azure Blob
rm record.tsv record.sorted.tsv record.anime.tsv subject.tsv
echo "Upload ranking result."
az storage file upload --share-name bangumi-publish/ranking --source customrank.csv -p customrank_$(date +"%Y_%m_%d").csv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
