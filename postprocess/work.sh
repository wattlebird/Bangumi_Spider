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
join -12 -22 -t$'\t' -o 2.1,1.1,2.3,2.4,2.5,2.6,2.7,2.8 subject.left record.right | sort -t$'\t' -k1,1n -k2,2n -u > record_"$aujourdhui".tsv
# Remove duplicated subject in subject.tsv
awk -F "\t" '$1==$2 {print $0;}' < subject.sorted | cut -f2 --complement | sort -t$'\t' -k1,1 -u > subject_"$aujourdhui".tsv
echo "Generating staffs table"
gawk -F "\t" '$9 {split($9, staffs, ";"); for(s in staffs) { split(staffs[s], people, /:|,/); for(p in people) { if (p!=1 && people[1]) printf("%s\t%s\t%s\t%s\n", $1, $4, people[1], people[p]);} } }' subject_"$aujourdhui".tsv > staffs_"$aujourdhui".tsv
echo "Generating tags table"
gawk -F "\t" '$7 {split($7, tags, ";"); for(tag in tags) if (tags[tag]) {printf("%s\t%s\t%s\t%s\t%s\n", $1, $2, $3, $5, tags[tag]);}}' record_"$aujourdhui".tsv | sort -u > tags_"$aujourdhui".tsv
python customtags.py tags_"$aujourdhui".tsv
echo "Cleaning up..."
rm subject.left subject.sorted record.right

# publish?
echo "Publish data."
az storage file upload --share-name bangumi-publish/subject --source subject_"$aujourdhui".tsv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
az storage file upload --share-name bangumi-publish/record --source record_"$aujourdhui".tsv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
az storage file upload --share-name bangumi-publish/tags --source tags_"$aujourdhui".tsv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
az storage file upload --share-name bangumi-publish/staffs --source staffs_"$aujourdhui".tsv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
az storage file upload --share-name bangumi-publish/tags --source customtags_"$aujourdhui".tsv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT

# 1. generate averaged score pair on avg, cdf and prob normalization methods.
echo "Generating paired scores."
python generate_matrix.py record_"$aujourdhui".tsv
# 2. calculate custom rank using rankit.
echo "Calculating rank."
awk -F "\t" '$4=="anime" && length($5)!=0 {printf("%d\t%s\t%d\n", $1, $2, $5)}' subject_"$aujourdhui".tsv > subject.tsv
python customrank.py
# 3. upload the custom rank to Azure Blob
echo "Upload ranking result."
az storage file upload --share-name bangumi-publish/ranking --source customrank.csv -p customrank_$(date +"%Y_%m_%d").csv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
