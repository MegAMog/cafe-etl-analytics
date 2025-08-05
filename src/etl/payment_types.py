import pandas as pd
import utils.uuid_str as u

#Create products table
def transform_payment_types(data:pd.DataFrame) -> pd.DataFrame:
    payment_type = data[['payment_type_id','payment_type']].copy()

    #1. Remove empty/NULL values
    payment_type.dropna(inplace = True)

    #2. Remove duplicates
    payment_type.drop_duplicates(inplace = True)

    return payment_type