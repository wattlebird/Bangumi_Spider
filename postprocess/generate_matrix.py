import pandas as pd
import numpy as np
import argparse
from pyarrow import json

parser = argparse.ArgumentParser()
parser.add_argument('recordfile', type=str)
parser.add_argument('subjectfile', type=str)

step = 1000

def main(record_file, subject_file):
    record_js = json.read_json(record_file)
    records = record_js.to_pandas()
    del record_js
    records.drop(columns=['updated_at', 'comment', 'vol_status', 'ep_status', 'subject_type', 'type', 'tags', 'private', 'user_id'], inplace=True)
    subjects = pd.read_json(subject_file, lines=True)

    subjects['name_merged'] = subjects.apply(lambda x: x['name'] if pd.isna(x['name_cn']) else x['name_cn'], axis=1)
    subjects = subjects[(subjects['type'] == 'anime') & (~pd.isnull(subjects['rank']) | (subjects['rate_count'] >= 50))]
    subjects.to_csv("subject.tsv", sep='\t', index=False, header=None, columns=['id', 'name_merged', 'rank'])
    print("anime (with rank) only subject.tsv are generated")

    records = records.merge(subjects[['id']], left_on='subject_id', right_on='id')
    records = records[records['rate'] != 0]

    user_cdf = records.groupby(by=['user_order', 'rate'], as_index=False)['subject_id'].count().rename(columns={'subject_id': 'cnt'}).sort_values(by=['user_order', 'rate'], ignore_index=True)
    user_cdf['cumsum'] = user_cdf.groupby(by='user_order')['cnt'].cumsum()
    user_cdf['cumsum_max'] = user_cdf.groupby(by='user_order')['cumsum'].transform(pd.Series.max)
    user_cdf['cdf'] = user_cdf['cumsum'] / user_cdf['cumsum_max']
    user_cdf.drop(columns=['cnt', 'cumsum', 'cumsum_max'], inplace=True)
    records = records.merge(user_cdf, how='left', on=['user_order', 'rate'])
    records['rate'] = records['rate'].astype('float32')
    records['cdf'] = records['cdf'].astype('float32')

    records = records.merge(subjects.id, left_on='subject_id', right_on='id', how='inner')

    itemLen = int(records.subject_id.max())
    print("start generating mat file, maximal item id {0}".format(itemLen))

    avg = []
    prob = []
    cdf = []

    for i in range(itemLen // step):
        record_piece_l = records[(records.subject_id >= step * i) & (records.subject_id < step * (i+1))]
        for j in range(itemLen // step):
            if i > j:
                continue;
            
            record_piece_r = records[(records.subject_id >= step * j) & (records.subject_id < step * (j+1))]
            records_piece_x = record_piece_l.merge(record_piece_r, how='outer', on='user_order', suffixes=('_l', '_r'))
            records_piece_x = records_piece_x[records_piece_x.subject_id_l < records_piece_x.subject_id_r]
            records_piece_x['prob_l'] = np.require(records_piece_x['rate_l'] > records_piece_x['rate_r'], dtype='float32')
            records_piece_x['prob_r'] = np.require(records_piece_x['rate_l'] < records_piece_x['rate_r'], dtype='float32')
            avg.append(records_piece_x.groupby(by=['subject_id_l', 'subject_id_r'], as_index=False)[['rate_l', 'rate_r']].mean())
            prob.append(records_piece_x.groupby(by=['subject_id_l', 'subject_id_r'], as_index=False)[['prob_l', 'prob_r']].mean())
            cdf.append(records_piece_x.groupby(by=['subject_id_l', 'subject_id_r'], as_index=False)[['cdf_l', 'cdf_r']].mean())
            print("mat file {0} {1} generated.".format(i, j))
    
    pd.concat(avg, ignore_index=True).to_csv("pair.avg.tsv", sep='\t', index=False, header=None)
    pd.concat(prob, ignore_index=True).to_csv("pair.prob.tsv", sep='\t', index=False, header=None)
    pd.concat(cdf, ignore_index=True).to_csv("pair.cdf.tsv", sep='\t', index=False, header=None)
    print("finished writing files")


if __name__=="__main__":
    args = parser.parse_args()
    main(args.recordfile, args.subjectfile)