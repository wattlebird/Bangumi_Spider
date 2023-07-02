from dateparser.date import DateDataParser
import json
import pandas as pd
from datetime import date
import argparse
from azure.storage.blob import BlobServiceClient
import os

parser = argparse.ArgumentParser()
parser.add_argument('recordfile', type=str)

def main(recordfile):
    print("Function comprehensive.py")
    today = date.today()
    azure_blob_key = os.getenv("AZURE_STORAGE_IKELY_KEY")
    azure_blob_account = os.getenv("AZURE_STORAGE_IKELY_ACCOUNT")
    blobServiceClient = BlobServiceClient(account_url=f"https://{azure_blob_account}.blob.core.windows.net/", credential=azure_blob_key)
    cur_ids = set()
    pre_ids = set()

    with open(recordfile, "r") as fp:
        lines = []
        for count, line in enumerate(fp):
            rec = json.loads(line)
            id = f"{rec['subject_id']}_{rec['user_order']}"
            cur_ids.add(id)
            lines.append(line)
            if count % 10000 == 0:
                with open("./tmp.jsonlines", "w") as fw:
                    fw.writelines(lines)
                filename = f"collections_{today.strftime('%Y_%m_%d')}.{count//10000:05d}.jsonlines"
                blobClient = blobServiceClient.get_blob_client(container="elastic", blob=filename)
                with open("./tmp.jsonlines", "rb") as data:
                    print(f"Uploading file {filename}")
                    blobClient.upload_blob(data)
                lines = []
        if len(lines) != 0:
            with open("./tmp.jsonlines", "w") as fw:
                fw.writelines(lines)
                filename = f"collections_{today.strftime('%Y_%m_%d')}.{count//10000:05d}.jsonlines"
                blobClient = blobServiceClient.get_blob_client(container="elastic", blob=filename)
                with open("./tmp.jsonlines", "rb") as data:
                    print(f"Uploading file {filename}")
                    blobClient.upload_blob(data)
    
    dbContainer = blobServiceClient.get_container_client("bangumi")
    collectionList = list(dbContainer.list_blobs(name_starts_with="collections-api"))
    collectionList.sort(key=lambda x: x.last_modified, reverse=True)
    prev = dbContainer.get_blob_client(collectionList[1])
    with open(file=collectionList[1].name, mode="wb") as sample_blob:
        print(f"Downloading file {collectionList[1].name}")
        download_stream = prev.download_blob()
        sample_blob.write(download_stream.readall())
    with open(collectionList[1].name, "r") as fp:
    #with open(recordfile, "r") as fp:
        lines = []
        for count, line in enumerate(fp):
            rec = json.loads(line)
            id = f"{rec['subject_id']}_{rec['user_order']}"
            pre_ids.add(id)
    to_be_del = pre_ids - cur_ids
    print(f"{len(to_be_del)} items to be removed from current db")
    pd.DataFrame({
        "id": list(to_be_del)
    }).to_json(f"collections_del_{today.strftime('%Y_%m_%d')}.jsonlines", orient='records', force_ascii=False, lines=True)
    blobClient = blobServiceClient.get_blob_client(container="elastic", blob=f"collections_del_{today.strftime('%Y_%m_%d')}.jsonlines")
    with open(f"collections_del_{today.strftime('%Y_%m_%d')}.jsonlines", "rb") as data:
        print(f"Uploading file collections_del_{today.strftime('%Y_%m_%d')}.jsonlines")
        blobClient.upload_blob(data)

if __name__=="__main__":
    args = parser.parse_args()
    main(args.recordfile)