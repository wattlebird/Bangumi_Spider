import pandas as pd
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('recordfile', type=str)
parser.add_argument('subjectfile', type=str)

step = 1000

def main(record_file, subject_file):
    records = pd.read_csv(record_file, sep='\t', usecols=['uid', 'iid', 'rate'])
    subjects = pd.read_json(subject_file, lines=True)

    subjects['name_merged'] = subjects.apply(lambda x: x['name'] if pd.isna(x['name_cn']) else x['name_cn'], axis=1)
    subjects = subjects[(subjects['type'] == 'anime') & (~pd.isnull(subjects['rank']) | (subjects['rate_count'] >= 50))]
    subjects.to_csv("subject.tsv", sep='\t', index=False, header=None, columns=['id', 'name_merged', 'rank'])
    print("anime (with rank) only subject.tsv are generated")

    records = records.merge(subjects[['id']], left_on='iid', right_on='id')
    records = records[~pd.isnull(records.rate)]

    user_cdf = records.groupby(by=['uid', 'rate'], as_index=False)['iid'].count().rename(columns={'iid': 'cnt'}).sort_values(by=['uid', 'rate'], ignore_index=True)
    user_cdf['cumsum'] = user_cdf.groupby(by='uid')['cnt'].cumsum()
    user_cdf['cumsum_max'] = user_cdf.groupby(by='uid')['cumsum'].transform(pd.Series.max)
    user_cdf['cdf'] = user_cdf['cumsum'] / user_cdf['cumsum_max']
    user_cdf.drop(columns=['cnt', 'cumsum', 'cumsum_max'], inplace=True)
    records = records.merge(user_cdf, how='left', on=['uid', 'rate'])
    records['rate'] = records['rate'].astype('float32')
    records['cdf'] = records['cdf'].astype('float32')

    records = records.merge(subjects.id, left_on='iid', right_on='id', how='inner')

    itemLen = int(records.iid.max())
    print("start generating mat file, maximal item id {0}".format(itemLen))

    avg = []
    prob = []
    cdf = []

    for i in range(itemLen // step):
        record_piece_l = records[(records.iid >= step * i) & (records.iid < step * (i+1))]
        for j in range(itemLen // step):
            if i > j:
                continue;
            
            record_piece_r = records[(records.iid >= step * j) & (records.iid < step * (j+1))]
            records_piece_x = record_piece_l.merge(record_piece_r, how='outer', on='uid', suffixes=('_l', '_r'))
            records_piece_x = records_piece_x[records_piece_x.iid_l < records_piece_x.iid_r]
            records_piece_x['prob_l'] = np.require(records_piece_x['rate_l'] > records_piece_x['rate_r'], dtype='float32')
            records_piece_x['prob_r'] = np.require(records_piece_x['rate_l'] < records_piece_x['rate_r'], dtype='float32')
            avg.append(records_piece_x.groupby(by=['iid_l', 'iid_r'], as_index=False)[['rate_l', 'rate_r']].mean())
            prob.append(records_piece_x.groupby(by=['iid_l', 'iid_r'], as_index=False)[['prob_l', 'prob_r']].mean())
            cdf.append(records_piece_x.groupby(by=['iid_l', 'iid_r'], as_index=False)[['cdf_l', 'cdf_r']].mean())
            print("mat file {0} {1} generated.".format(i, j))
    
    pd.concat(avg, ignore_index=True).to_csv("pair.avg.tsv", sep='\t', index=False, header=None)
    pd.concat(prob, ignore_index=True).to_csv("pair.prob.tsv", sep='\t', index=False, header=None)
    pd.concat(cdf, ignore_index=True).to_csv("pair.cdf.tsv", sep='\t', index=False, header=None)
    print("finished writing files")


if __name__=="__main__":
    args = parser.parse_args()
    main(args.recordfile, args.subjectfile)