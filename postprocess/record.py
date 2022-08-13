import pandas as pd
import argparse        
from opencc import OpenCC
import re
import json
from datetime import date
from wikiparser import parse, WikiSyntaxError

parser = argparse.ArgumentParser()
parser.add_argument('recordfile', type=str)
parser.add_argument('subjectfile', type=str)
parser.add_argument('subjectarchive', type=str)

def info_to_json(info):
    try:
        return json.dumps(parse(info.replace("\x00", "")).info, ensure_ascii=False)
    except WikiSyntaxError as err:
        return "[]"

def get_type(s):
    if s:
        typ = re.search(r"Infobox(\s([/\w]+))?\s?\r?\n", s).group(2)
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

def resolve_type(r):
    if not pd.isnull(r['subjecttype']):
        return r['subjecttype']
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

def main(fRec, fSub, fSubAr):
    print("Function record.py")
    today = date.today()
    record_raw = pd.read_csv(fRec, sep='\t', parse_dates=['adddate'], error_bad_lines=False, warn_bad_lines=True)
    print(f"Read file {fRec}, altogether {record_raw.shape[0]} lines.")
    subject_raw = pd.read_csv(fSub, sep='\t', dtype={'rank': 'Int32'}, error_bad_lines=False, warn_bad_lines=True)
    print(f"Read file {fSub}, altogether {subject_raw.shape[0]} lines.")
    subject_ar = pd.read_json(fSubAr, lines=True)
    print(f"Read file {fSubAr}, altogether {subject_ar.shape[0]} lines.")

    subject_ar = subject_ar.merge(subject_raw[['order', 'subjecttype', 'rank']], how='left', left_on='id', right_on='order')

    subject_ar['type_parsed'] = subject_ar['infobox'].apply(get_type)
    subject_ar['type'] = subject_ar.apply(resolve_type, axis=1)
    subject_ar.drop(columns=['subjecttype', 'type_parsed', 'order'], inplace=True)
    print(f"Types and ranks generated for subjects.")

    subject_ar['infobox'] = subject_ar['infobox'].apply(info_to_json)
    subject_ar['summary'] = subject_ar['summary'].apply(lambda x: x.replace("\x00", ""))
    record_raw.drop_duplicates(subset=['uid', 'iid'], keep='last', inplace=True)

    fav_count = record_raw.groupby(by='iid')['uid'].count()
    rate_count = record_raw[~pd.isnull(record_raw.rate)].groupby(by='iid')['uid'].count()
    state_count = record_raw.groupby(by=['iid', 'state'])['uid'].count().unstack()
    subject_ar = subject_ar.merge(fav_count.rename('fav_count'), how='left', left_on='id', right_index=True)\
                            .merge(rate_count.rename('rate_count'), how='left', left_on='id', right_index=True)\
                            .merge(state_count.add_suffix('_count'), how='left', left_on='id', right_index=True)
    subject_ar.fillna({'fav_count': 0, 'rate_count': 0, 'collect_count': 0, 'do_count': 0, 'dropped_count': 0, 'on_hold_count': 0, 'wish_count': 0}, inplace=True)
    subject_ar = subject_ar.astype({'fav_count': int, 'rate_count': int, 'collect_count': int, 'do_count': int, 'dropped_count': int, 'on_hold_count': int, 'wish_count': int})
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
    main(args.recordfile, args.subjectfile, args.subjectarchive)