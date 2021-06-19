import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('tagfile', type=str)

def main(tagfile):
    tag_df = pd.read_csv(tagfile, sep='\t', header=None, names=['uid', 'iid', 'typ', 'date', 'tag'])
    tag_df.drop(columns=['date'], inplace=True)

    tag_count = tag_df.groupby(by=['iid'], as_index=False)['uid'].nunique().rename(columns={'uid': 'user_count'})
    tag_user_count = tag_df.groupby(by=['iid', 'tag'], as_index=False)['uid'].nunique().rename(columns={'uid': 'tag_count'})
    subject_tag_df = tag_user_count.merge(tag_count, on='iid', how='left')
    subject_tag_df['tag_confidence'] = subject_tag_df['tag_count'] / subject_tag_df['user_count']

    user_authority = pd.DataFrame({
        'uid': tag_df['uid'].drop_duplicates().reset_index(drop=True),
        'authority': 1.0
    })
    print("start generating tag probability...")

    for i in range(3):
        nxt = tag_df.merge(user_authority, how='left', on='uid')\
                .merge(subject_tag_df, how='left', on=['iid', 'tag'])
        nxt['tag_confidence'] *= nxt['authority']
        subject_tag_score = nxt.groupby(by=['iid', 'tag'])['tag_confidence'].mean()

        nxt.drop(columns=['tag_confidence'], inplace=True)
        nxt = nxt.merge(subject_tag_score, how='left', left_on=['iid', 'tag'], right_index=True)
        temp = nxt.groupby(by=['uid', 'iid'])['tag_confidence'].max().to_frame()
        user_authority = temp.groupby(by='uid')['tag_confidence'].mean().rename('authority').to_frame().reset_index()
        print("iteration {0} finished".format(i+1))
    
    nxt = tag_df.merge(user_authority, how='left', on='uid')\
            .merge(subject_tag_df, how='left', on=['iid', 'tag'])
    nxt['tag_confidence'] *= nxt['authority']
    subject_tag_score = nxt.groupby(by=['iid', 'tag'])['tag_confidence'].mean()

    subject_tag_df.drop(columns=['tag_confidence']).merge(subject_tag_score, how='left', left_on=['iid', 'tag'], right_index=True).to_csv("customtags_"+tagfile[-14:], sep='\t', index=False, header=True)
    print("written to tag tsv")

if __name__=="__main__":
    args = parser.parse_args()
    main(args.tagfile)