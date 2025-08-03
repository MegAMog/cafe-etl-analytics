import pandas as pd
import utils.uuid as u

#Create transactions table
def transform_transactions(data:pd.DataFrame) -> pd.DataFrame:
    transactions = data.copy()
    
    drop_columns = ['order_snapshot', 'payment_type', 'branch_name']
    transactions.drop(columns = drop_columns, inplace=True)

    return transactions