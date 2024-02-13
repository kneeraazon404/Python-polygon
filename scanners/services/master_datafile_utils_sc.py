import pandas as pd

def clean_master_df(master_df):

    # STEP 1: Import file and drop all nan vlaues
    master_df = master_df.dropna(axis=1, how='all')

    # STEP 2: convert date column to something indexable
    master_df['date'] = pd.to_datetime(master_df['date'])

    # STEP 3: Iterate through each column and clean/convert formatting.
    for (columnName, columnData) in master_df.iteritems():
        if (master_df[columnName].dtypes == object):

            # 3a. Convert columns with % signs to float for Pandas
            if not master_df[columnName][master_df[columnName].str.contains('%', na=False)].empty:
                master_df[columnName] = master_df[columnName].str.rstrip('%').astype('float') / 100.0  # format %s to floats

            # 3b. Convert columns with $ and negative balances to - signs and to float -> for when we use 'account/finance' format in excel
            elif not master_df[columnName][master_df[columnName].str.contains('\$', na=False)].empty:
                master_df[columnName] = master_df[columnName].str\
                    .replace('[\$,)]', '', regex=True)\
                    .replace('[(,]', '-', regex=True)\
                    .astype(float)  # format %s to floats

    return master_df