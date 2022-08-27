import numpy as np
import pandas as pd
import gc
from rankit.Table import Table
from rankit.Ranker import MasseyRanker, KeenerRanker, ODRanker, MarkovRanker
from rankit.Merge import borda_count_merge

gc.enable()

avg = pd.read_csv('pair.avg.tsv', header=None, delimiter='\t', names=['item1', 'item2', 'score1', 'score2'], dtype={
    'item1': np.uint32,
    'item2': np.uint32,
    'score1': np.float32,
    'score2': np.float32
})

cdf = pd.read_csv('pair.cdf.tsv', header=None, delimiter='\t', names=['item1', 'item2', 'score1', 'score2'], dtype={
    'item1': np.uint32,
    'item2': np.uint32,
    'score1': np.float32,
    'score2': np.float32
})

prob = pd.read_csv('pair.prob.tsv', header=None, delimiter='\t', names=['item1', 'item2', 'score1', 'score2'], dtype={
    'item1': np.uint32,
    'item2': np.uint32,
    'score1': np.float32,
    'score2': np.float32
})



data = Table(avg, col = ['item1', 'item2', 'score1', 'score2'])
ranker = MasseyRanker()
masseyRank = ranker.rank(data)
keener = KeenerRanker()
keenerRank = keener.rank(data)
od = ODRanker()
odRank = od.rank(data)
markov = MarkovRanker()
markovRank = markov.rank(data)

data = Table(cdf, col = ['item1', 'item2', 'score1', 'score2'])
ranker = MasseyRanker()
masseyCdfRank = ranker.rank(data)
keener = KeenerRanker()
keenerCdfRank = keener.rank(data)
od = ODRanker()
odCdfRank = od.rank(data)
markov = MarkovRanker()
markovCdfRank = markov.rank(data)

data = Table(prob, col = ['item1', 'item2', 'score1', 'score2'])
ranker = MasseyRanker()
masseyProbRank = ranker.rank(data)
keener = KeenerRanker()
keenerProbRank = keener.rank(data)
od = ODRanker()
odProbRank = od.rank(data)
markov = MarkovRanker()
markovProbRank = markov.rank(data)


mergedRank = borda_count_merge([masseyRank, keenerRank, odRank, markovRank, masseyCdfRank, 
keenerCdfRank, odCdfRank, markovCdfRank, masseyProbRank, keenerProbRank, odProbRank, markovProbRank])

subject = pd.read_csv("subject.tsv", delimiter='\t', dtype={
    'iid': 'uint32'
}, header=None, names=['iid', 'subjectname', 'rank'], engine='c')

table = mergedRank.merge(subject, left_on='name', right_on='iid', suffixes=('_custom', '_bangumi'))
table['rank_bangumi'] = table.rank_bangumi.astype('UInt32')
table.to_csv("customrank.csv", columns=['name', 'rank_custom', 'rank_bangumi', 'subjectname'], index=False, header=True)