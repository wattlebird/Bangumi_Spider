#!/bin/bash
# fetch record and subject file from azure storage service

echo "Begining fetching filename..."

recordfile=$(az storage blob list -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -o table --prefix collections | sed 1,2d | tr -s '\040' '\011' | cut -f1,6 | sort -t$'\t' -k2,2r | sed 1q | sed 's/\t.*//g')
echo "Record file read as ${recordfile}."

subjectfile=$(az storage blob list -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -o table --prefix subject | sed 1,2d | tr -s '\040' '\011' | cut -f1,6 | sort -t$'\t' -k2,2r | sed 1q | sed 's/\t.*//g')
echo "API scraped subject file read as ${subjectfile}."


az storage blob download -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -n "$recordfile" -f "$recordfile" && echo "Record file downloaded under ${PWD}."
az storage blob download -c bangumi --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT -n "$subjectfile" -f "$subjectfile" && echo "Subject file downloaded under ${PWD}."
aujourdhui=$(date +"%Y_%m_%d")

curl -L -o dump.zip $(curl 'https://api.github.com/repos/bangumi/Archive/releases' | jq '.[0].assets|max_by(.created_at)|.browser_download_url' | tr -d '"')
unzip dump.zip -d dump

python record.py $subjectfile dump/subject.jsonlines
python customtags.py $recordfile dump/subject.jsonlines

# publish?
echo "Publish data."
az storage blob upload -c database -f subject_archive_"$aujourdhui".jsonlines --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
az storage blob upload -c database -f subject_entity_"$aujourdhui".jsonlines --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT
az storage blob upload -c database -f tags_"$aujourdhui".jsonlines --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT

# 1. generate averaged score pair on avg, cdf and prob normalization methods.
echo "Generating paired scores."
python generate_matrix.py $recordfile subject_archive_"$aujourdhui".jsonlines
# 2. calculate custom rank using rankit.
echo "Calculating rank."
python customrank.py
# 3. upload the custom rank to Azure Blob
echo "Upload ranking result."
az storage blob upload -c database -f customrank.csv -n customrank_$(date +"%Y_%m_%d").csv --account-key $AZURE_STORAGE_IKELY_KEY --account-name $AZURE_STORAGE_IKELY_ACCOUNT

echo "Generating comprehensive subject archive"
python comprehensive.py subject_archive_"$aujourdhui".jsonlines tags_"$aujourdhui".jsonlines customrank.csv
