import pandas as pd

#Create products table
def transform_branch(data:pd.DataFrame) -> pd.DataFrame:
    branch = data[['branch_id','branch_name']].copy()

    #1. Remove empty/NULL values
    branch.dropna(inplace = True)

    #2. Remove duplicates
    branch.drop_duplicates(inplace = True)

    return branch
