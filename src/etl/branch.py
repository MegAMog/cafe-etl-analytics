import pandas as pd
import utils.uuid as u

#Create products table
def transfrom_branch(data:pd.DataFrame) -> pd.DataFrame:
    branch = data['branch_name'].copy().to_frame()
    print(type(branch))

    #1. Remove empty/NULL values
    branch.dropna(inplace = True)

    #2. Remove duplicates
    branch.drop_duplicates(inplace = True)

    #3. Add branch_id 
    branch['branch_id'] = branch.apply(
        lambda row: u.create_uuid_from_list([row['branch_name']]),
        axis=1
    )

    return branch
