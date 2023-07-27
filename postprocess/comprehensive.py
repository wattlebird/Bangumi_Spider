from dateparser.date import DateDataParser
import json
import pandas as pd
from datetime import date, datetime
import argparse
from azure.storage.blob import BlobServiceClient
import os
import re

parser = argparse.ArgumentParser()
parser.add_argument('subjectentityfile', type=str)
parser.add_argument('tagsfile', type=str)
parser.add_argument('rankingfile', type=str)

ddp = DateDataParser(settings={'PREFER_DAY_OF_MONTH': 'last', 'REQUIRE_PARTS': ['year'], 'DEFAULT_LANGUAGES': ["en"]}, languages=['en', 'zh'])
properties_rev = {
    "中文名": "name_cn",
    "集数": "num_episodes",
    "放送星期": "week_onair",
    "开始": "date_onair",
    "结束": "date_onair_end",
    "类型": "showtype",
    "国家/地区": "country",
    "语言": "language",
    "每集长": "length_per_episode",
    "频道": "channel",
    "电视网": "tvnetwork",
    "电视台": "tvstation",
    "官方网站": "website",
    "游戏类型": "gametype",
    "游戏引擎": "engine",
    "游玩人数": "num_players",
    "发行日期": "date_release",
    "售价": "price",
    "website": "website",
    "版本特性": "version_characteristics",
    "发售日期": "date_release",
    "价格": "price",
    "播放时长": "length",
    "录音": "recorder",
    "碟片数量": "num_disks",
    "话数": "num_episodes",
    "放送开始": "date_onair",
    "播放电视台": "tvstation",
    "其他电视台": "tvstation_other",
    "播放结束": "date_onair_end",
    "Copyright": "copyright",
    "发售日": "date_release",
    "上映年度": "year_onair",
    "片长": "length",
    "出版社": "publisher",
    "其他出版社": "publisher_other",
    "连载杂志": "magazine",
    "册数": "num_volumes",
    "页数": "num_pages",
    "ISBN": "isbn",
}

def extract_alias(rec):
    infobox_json = json.loads(rec)
    if infobox_json == None:
        return None
    for item in infobox_json:
        if item['key'] == '别名':
            if type(item['value']) == list and len(item['value']) != 0:
                return list(map(lambda x: x['v'], item['value']))
            elif type(item['value']) == str:
                return [item['value']]
            else:
                return None

def extract_platform(rec):
    infobox_json = json.loads(rec)
    if infobox_json == None:
        return None
    for item in infobox_json:
        if item['key'] == '平台':
            if type(item['value']) == list and len(item['value']) != 0:
                return list(map(lambda x: x['v'], item['value']))
            elif type(item['value']) == str:
                return [item['value']]
            else:
                return None

def normalize_infobox(rec):
    infobox_json = json.loads(rec)
    rtn = {}
    if infobox_json == None:
        return rtn
    for item in infobox_json:
        if item['key'] in properties_rev and type(item['value']) != list and item['value']:
            k = properties_rev[item['key']]
            if k.startswith("date_"):
                date = dateparser_hotfix(item['value'].strip())
                if date != None:
                    rtn[properties_rev[item['key']]] = date.strftime("%Y-%m-%d")
                else:
                    v = ddp.get_date_data(item['value'])
                    if v.date_obj != None:
                        rtn[properties_rev[item['key']]] = v.date_obj.strftime("%Y-%m-%d")
            else:
                rtn[properties_rev[item['key']]] = item['value']
    return rtn

def extract_suggest(rec):
    suggest = [{'input': rec['name'], 'weight': max(1, rec['fav_count'])}]
    if rec.name_cn:
        suggest.append({'input': rec['name_cn'], 'weight': max(1, rec['fav_count'])})
    if type(rec.alias) == list:
        for alias in rec.alias:
            suggest.append({'input': alias, 'weight': max(1, rec['fav_count'] // 2)})
    if type(rec.tags) == list:
        for tag in rec.tags:
            if tag['tag_cnt'] > 1:
                suggest.append({'input': tag['tags'], 'weight': tag['tag_cnt']})
    return suggest

def dateparser_hotfix(s):
    # Extract the number from the regex pattern
    match = re.match(r'^(\d+)年$', s)
    if match:
        year = int(match.group(1))
        
        # Get the current date
        current_date = datetime.now().date()
        
        # Create a new date object with the extracted year
        converted_date = current_date.replace(year=year)
        
        return converted_date
    
    return None

def main(subjectentityfile, tagsfile, rankingfile):
    print("Function comprehensive.py")
    today = date.today()
    azure_blob_key = os.getenv("AZURE_STORAGE_IKELY_KEY")
    azure_blob_account = os.getenv("AZURE_STORAGE_IKELY_ACCOUNT")
    blobServiceClient = BlobServiceClient(account_url=f"https://{azure_blob_account}.blob.core.windows.net/", credential=azure_blob_key)

    subject_entity = pd.read_json(subjectentityfile, lines=True)
    tags = pd.read_json(tagsfile, lines=True)
    ranking = pd.read_csv(rankingfile)
    subject_tags = tags.drop(columns=['tags_normalized', 'tagnorm_cnt']).groupby('iid').apply(lambda x: x.to_dict(orient='records'))
    subject_comprehensive = subject_entity\
        .merge(subject_tags.rename('tags'), how='left', left_on='id', right_index=True)\
        .merge(ranking.drop(columns=['rank_bangumi', 'subjectname']).rename(columns={'name': 'id'}), how='left', left_on='id', right_on='id')
    subject_comprehensive['alias'] = subject_comprehensive['infobox'].apply(extract_alias)
    subject_comprehensive['platform'] = subject_comprehensive['infobox'].apply(extract_platform)
    subject_comprehensive['infobox'] = subject_comprehensive['infobox'].apply(normalize_infobox)
    subject_comprehensive['suggest'] = subject_comprehensive.apply(extract_suggest, axis=1)

    step = 1000
    for i, b in enumerate(range(0, subject_comprehensive.shape[0], 1000)):
        e = min(b+1000, subject_comprehensive.shape[0])
        piece = subject_comprehensive.iloc[b:e]
        filename = f"subject_comprehensive_{today.strftime('%Y_%m_%d')}.{i:04d}.jsonlines"
        piece.to_json("./tmp.jsonlines", orient='records', force_ascii=False, lines=True)
        blobClient = blobServiceClient.get_blob_client(container="elastic", blob=filename)
        with open("./tmp.jsonlines", "rb") as data:
            print(f"Uploading file {filename}")
            blobClient.upload_blob(data)
    
    dbContainer = blobServiceClient.get_container_client("database")
    subjectList = list(dbContainer.list_blobs(name_starts_with="subject_archive"))
    subjectList.sort(key=lambda x: x.last_modified, reverse=True)
    prev = dbContainer.get_blob_client(subjectList[1])
    with open(file=subjectList[1].name, mode="wb") as sample_blob:
        print(f"Downloading file {subjectList[1].name}")
        download_stream = prev.download_blob()
        sample_blob.write(download_stream.readall())
    prev_subject = pd.read_json(subjectList[1].name, lines=True)
    to_be_del = set(prev_subject['id']) - set(subject_comprehensive['id'])
    print(f"{len(to_be_del)} items to be removed from current db")
    pd.DataFrame({
        "id": list(to_be_del)
    }).to_json(f"subject_del_{today.strftime('%Y_%m_%d')}.jsonlines", orient='records', force_ascii=False, lines=True)
    blobClient = blobServiceClient.get_blob_client(container="elastic", blob=f"subject_del_{today.strftime('%Y_%m_%d')}.jsonlines")
    with open(f"subject_del_{today.strftime('%Y_%m_%d')}.jsonlines", "rb") as data:
        print(f"Uploading file subject_del_{today.strftime('%Y_%m_%d')}.jsonlines")
        blobClient.upload_blob(data)

if __name__=="__main__":
    args = parser.parse_args()
    main(args.subjectentityfile, args.tagsfile, args.rankingfile)
