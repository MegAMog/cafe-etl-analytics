import pandas as pd
import utils.uuid as u

#Create products table
def transform_payment_types(data:pd.DataFrame) -> pd.DataFrame:
    payment_type = data['payment_type'].copy().to_frame()

    #1. Remove empty/NULL values
    payment_type.dropna(inplace = True)

    #2. Remove duplicates
    payment_type.drop_duplicates(inplace = True)

    #3. Add payment_type_id 
    payment_type['payment_type_id'] = payment_type.apply(
        lambda row: u.create_uuid_from_list([row['payment_type']]),
        axis=1
    )

    return payment_type