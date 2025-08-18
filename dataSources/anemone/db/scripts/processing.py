import pandas as pd
import numpy as np

def dwcAugment(df: pd.DataFrame) -> pd.DataFrame:

    matching = df['source_mat_id'].apply(lambda x: df.index[df['source_mat_id'] == x].tolist())
    df['haplotype'] = [lst.index(idx)+1 for idx, lst in enumerate(matching)]
    df['occurrenceID'] = df[['organismID', 'source_mat_id', 'haplotype']].apply(lambda x: '_'.join(y.strip('unidentified ') for y in x.values.astype(str)), axis=1)

    for col in df.columns:
        df[col] = df[col].replace('unidentified .*', np.NaN, regex=True)

    return df
