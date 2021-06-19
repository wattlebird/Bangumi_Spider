import pandas as pd
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('recordfile', type=str)

def main(record_file):
    records = pd.read_csv(record_file, sep='\t', header=None, names=['uid', 'iid', 'type', 'status', 'date', 'rate', 'tags', 'comment'], usecols=['uid', 'iid', 'type', 'rate'])

    records = records[(~pd.isna(records.rate)) & (records.type=='anime')]

    user_cdf = records.groupby(by=['uid', 'rate'], as_index=False)['iid'].count().rename(columns={'iid': 'cnt'}).sort_values(by=['uid', 'rate'], ignore_index=True)
    user_cdf['cumsum'] = user_cdf.groupby(by='uid')['cnt'].cumsum()
    user_cdf['cumsum_max'] = user_cdf.groupby(by='uid')['cumsum'].transform(pd.Series.max)
    user_cdf['cdf'] = user_cdf['cumsum'] / user_cdf['cumsum_max']
    user_cdf.drop(columns=['cnt', 'cumsum', 'cumsum_max'], inplace=True)
    records = records.merge(user_cdf, how='left', on=['uid', 'rate'])
    records['rate'] = records['rate'].astype('float32')
    records['cdf'] = records['cdf'].astype('float32')

    itemLen = int(records.iid.max())
    print("start generating mat file, maximal item id {0}".format(itemLen))

    avg = []
    prob = []
    cdf = []

    for i in range(itemLen // 1000):
        record_piece_l = records[(records.iid >= 1000 * i) & (records.iid < 1000 * (i+1))]
        for j in range(itemLen // 1000):
            if i > j:
                continue;
            
            record_piece_r = records[(records.iid >= 1000 * j) & (records.iid < 1000 * (j+1))]
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
    main(args.recordfile)