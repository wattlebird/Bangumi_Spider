import re
import argparse
from datetime import date
import pandas as pd
import numpy as np
from numpy.linalg import norm
from opencc import OpenCC

converter = OpenCC('t2s')

def normalize(tag):
    rtn = re.sub("\W", "", tag)
    rtn = rtn.lower()
    rtn = converter.convert(rtn)
    return rtn

parser = argparse.ArgumentParser()
parser.add_argument('recordfile', type=str)
parser.add_argument('subjectarchive', type=str)

def main(recordfile, subjectarchive):
    print("Function customtags.py")
    today = date.today()
    record_df = pd.read_csv(recordfile, sep='\t', usecols=['uid', 'iid', 'tags'], error_bad_lines=False, warn_bad_lines=True)
    print(f"Read file {recordfile}, altogether {record_df.shape[0]} lines.")
    subject_ar = pd.read_json(subjectarchive, lines=True)
    print(f"Read file {subjectarchive}, altogether {subject_ar.shape[0]} lines.")

    record_df = record_df.merge(subject_ar[['id']], how='left', left_on='iid', right_on='id')
    record_df_filterred = record_df[~pd.isnull(record_df.id) & ~pd.isnull(record_df.tags)].reset_index(drop=True).drop(columns=['id'])
    record_df_filterred['tags'] = record_df_filterred['tags'].apply(lambda x: x.split(';'))
    tags = record_df_filterred.explode('tags', ignore_index=True)
    tags['tags'] = tags['tags'].apply(lambda x: x.replace("\x00", ""))
    tags['tags_normalized'] = tags.tags.apply(normalize)
    print(f"Normalized tags generated.")

    ua = pd.DataFrame({
        'uid': tags.uid.drop_duplicates(),
        'score': 1.0
    })
    ua.set_index('uid', inplace=True)
    ua = ua / ua['score'].sum()
    pv = ua['score'].to_numpy()
    cnt = 0
    while True:
        cnt += 1
        print(f"Calculating user authority, iteration {cnt}...")
        dot_prod = tags.merge(ua, left_on='uid', right_index=True)
        tags_hub = dot_prod.groupby(by=['iid', 'tags_normalized'])['score'].sum()
        tags_hub = tags_hub / tags_hub.sum()
        dot_prod = tags.merge(tags_hub, left_on=['iid', 'tags_normalized'], right_index=True)
        user_score = dot_prod.groupby(by='uid')['score'].sum()
        user_score = user_score / user_score.sum()
        ua = user_score.to_frame()
        v = ua['score'].to_numpy()
        if norm(pv - v) < 1e-6:
            print(f"User authority get.")
            break
        pv = v
    
    dot_prod = tags.merge(ua, left_on='uid', right_index=True)
    tags_hub = dot_prod.groupby(by=['iid', 'tags_normalized']).aggregate({'score': ['sum', 'count']})
    tags_hub.columns = ['confidence', 'tagnorm_cnt']
    tag_cnt = tags.groupby(by=['iid', 'tags'])['uid'].count().rename('tag_cnt')
    tags = tags.merge(tag_cnt, left_on=['iid', 'tags'], right_index=True)
    tags = tags.merge(tags_hub, left_on=['iid', 'tags_normalized'], right_index=True)
    tags = tags[['iid', 'tags', 'tags_normalized', 'confidence', 'tag_cnt', 'tagnorm_cnt']].drop_duplicates(ignore_index=True)
    tags.to_json(f"tags_{today.strftime('%Y_%m_%d')}.jsonlines", orient='records', force_ascii=False, lines=True)
    print(f"Tag file generated as tags_{today.strftime('%Y_%m_%d')}.jsonlines, altogether {tags.shape[0]} lines.")

if __name__=="__main__":
    args = parser.parse_args()
    main(args.recordfile, args.subjectarchive)