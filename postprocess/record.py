import pandas as pd
import argparse        
from opencc import OpenCC
import re
import json
from datetime import date
from wikiparser import parse, WikiSyntaxError

parser = argparse.ArgumentParser()
parser.add_argument('subjectfile', type=str)
parser.add_argument('subjectarchive', type=str)

def info_to_json(info):
    try:
        return json.dumps(parse(info.replace("\x00", "")).info, ensure_ascii=False)
    except WikiSyntaxError as err:
        return "[]"

def get_type(s):
    if s:
        typeboxmatch = re.search(r"Infobox(\s([/\w]+))?\s?\r?\n", s)
        if typeboxmatch is None:
            return None
        typ = typeboxmatch.group(2)
        if typ is None or typ == 'None':
            return None
        if typ.startswith('animanga'):
            if typ.endswith('Manga') or typ.endswith('Novel') or typ.endswith('Book') or typ.endswith('BookSeries'):
                return 'book'
            else:
                return 'anime'
        else:
            if typ == 'Album':
                return 'music'
            elif typ == 'Game':
                return 'game'
            else:
                return 'real'
    else:
        return None

def get_detailed_type(s):
    if s:
        typeboxmatch = re.search(r"Infobox(\s([/\w]+))?\s?\r?\n", s)
        if typeboxmatch is None:
            return None
        typ = typeboxmatch.group(2)
        if typ is None or typ == 'None':
            return None
        return typ
    else:
        return None

def resolve_type(r):
    if not pd.isnull(r['type']):
        if r['type'] == 1:
            return 'book'
        if r['type'] == 2:
            return 'anime'
        if r['type'] == 3:
            return 'music'
        if r['type'] == 4:
            return 'game'
        if r['type'] == 6:
            return 'real'
    else:
        return r['type_parsed']

def get_alias(rec):
    alias = []
    if rec['name']:
        alias.append(rec['name'])
    if rec['name_cn']:
        alias.append(rec['name_cn'])
    if type(rec['infobox']) is list:
        for prop in rec['infobox']:
            if prop['key'] == '中文名' and type(prop['value']) is str and prop['value'] != "":
                alias.append(prop['value'])
            if prop['key'] == '中文名' and type(prop['value']) is list:
                alias.extend([x['v'] for x in prop['value'] if x['v']])
            if prop['key'] == '英文名' and type(prop['value']) is str and prop['value'] != "":
                alias.append(prop['value'])
            if prop['key'] == '英文名' and type(prop['value']) is list:
                alias.extend([x['v'] for x in prop['value'] if x['v']])
            if prop['key'] == '别名' and type(prop['value']) is str and prop['value'] != "":
                alias.append(prop['value'])
            if prop['key'] == '别名' and type(prop['value']) is list:
                alias.extend([x['v'] for x in prop['value'] if x['v']])
    return alias

converter = OpenCC('t2s')

def normalize(tag):
    rtn = re.sub("\W", "", tag)
    rtn = rtn.lower()
    rtn = converter.convert(rtn)
    return rtn

def main(fSub, fSubAr):
    print("Function record.py")
    today = date.today()
    subject_raw = pd.read_json(fSub, lines=True)
    print(f"Read file {fSub}, altogether {subject_raw.shape[0]} lines.")
    subject_ar = pd.read_json(fSubAr, lines=True)
    print(f"Read file {fSubAr}, altogether {subject_ar.shape[0]} lines.")

    subject_raw['wish_count'] = subject_raw['collection'].apply(lambda x: x['wish'])
    subject_raw['on_hold_count'] = subject_raw['collection'].apply(lambda x: x['on_hold'])
    subject_raw['dropped_count'] = subject_raw['collection'].apply(lambda x: x['dropped'])
    subject_raw['do_count'] = subject_raw['collection'].apply(lambda x: x['doing'])
    subject_raw['collect_count'] = subject_raw['collection'].apply(lambda x: x['collect'])
    subject_raw['fav_count'] = subject_raw['collection'].apply(lambda x: x['wish'] + x['on_hold'] + x['dropped'] + x['doing'] + x['collect'])
    subject_raw['rank'] = subject_raw['rating'].apply(lambda x: x['rank'])
    subject_raw['rate_count'] = subject_raw['rating'].apply(lambda x: x['total'])
    subject_ar = subject_ar.merge(subject_raw[['id', 'rank', 'wish_count', 'on_hold_count', 'dropped_count', 'do_count', 'collect_count', 'fav_count', 'rate_count']].groupby('id').first(), how='left', left_on='id', right_index=True)

    subject_ar['type_parsed'] = subject_ar['infobox'].apply(get_type)
    subject_ar['type'] = subject_ar.apply(resolve_type, axis=1)
    subject_ar['subtype'] = subject_ar['infobox'].apply(get_detailed_type)
    subject_ar.drop(columns=['type_parsed'], inplace=True)
    print(f"Types and ranks generated for subjects.")

    subject_ar['infobox'] = subject_ar['infobox'].apply(info_to_json)
    subject_ar['summary'] = subject_ar['summary'].apply(lambda x: x.replace("\x00", ""))
    
    subject_ar.fillna({'fav_count': 0, 'rate_count': 0, 'collect_count': 0, 'do_count': 0, 'dropped_count': 0, 'on_hold_count': 0, 'wish_count': 0, 'rank': 0}, inplace=True)
    subject_ar = subject_ar.astype({'fav_count': int, 'rate_count': int, 'collect_count': int, 'do_count': int, 'dropped_count': int, 'on_hold_count': int, 'wish_count': int, 'rank': int})
    subject_ar.loc[subject_ar['rank'] == 0, 'rank'] = pd.NA
    print(f"Statistics generated for subjects.")

    subject_ar.to_json(f"subject_archive_{today.strftime('%Y_%m_%d')}.jsonlines", orient='records', force_ascii=False, lines=True)
    print(f"Subject file generated as subject_archive_{today.strftime('%Y_%m_%d')}.jsonlines, altogether {subject_ar.shape[0]} lines.")

    alias_series = subject_ar.apply(get_alias, axis=1)
    alias_df = pd.DataFrame({
        'id': subject_ar['id'],
        'alias': alias_series
    }, columns=['id', 'alias'])
    alias_df = alias_df.explode('alias', ignore_index=True)
    alias_df['alias'] = alias_df['alias'].apply(lambda x: x.replace("\x00", ""))
    alias_df.drop_duplicates(ignore_index=True, inplace=True)
    alias_df['alias_normalized'] = alias_df.alias.apply(normalize)

    alias_df.to_json(f"subject_entity_{today.strftime('%Y_%m_%d')}.jsonlines", orient='records', force_ascii=False, lines=True)
    print(f"Subject entity file generated as subject_entity_{today.strftime('%Y_%m_%d')}.jsonlines, altogether {alias_df.shape[0]} lines.")

if __name__=="__main__":
    args = parser.parse_args()
    main(args.subjectfile, args.subjectarchive)