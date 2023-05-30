from dateparser.date import DateDataParser
import json
import pandas as pd
from datetime import date
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('subjectentityfile', type=str)
parser.add_argument('tagsfile', type=str)
parser.add_argument('rankingfile', type=str)

ddp = DateDataParser(settings={'PREFER_DAY_OF_MONTH': 'last', 'REQUIRE_PARTS': ['month', 'year'], 'DEFAULT_LANGUAGES': ["en"]}, languages=['en', 'zh'])
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
                v = ddp.get_date_data(item['value'])
                if v.date_obj != None:
                    rtn[properties_rev[item['key']]] = v.date_obj.strftime("%Y-%m-%d")
            else:
                rtn[properties_rev[item['key']]] = item['value']
    return rtn

def main(subjectentityfile, tagsfile, rankingfile):
    print("Function comprehensive.py")
    today = date.today()
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
    subject_comprehensive.to_json(f"subject_comprehensive_{today.strftime('%Y_%m_%d')}.jsonlines", orient='records', force_ascii=False, lines=True)

if __name__=="__main__":
    args = parser.parse_args()
    main(args.subjectentityfile, args.tagsfile, args.rankingfile)
