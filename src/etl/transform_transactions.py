import pandas as pd
import utils.uuid_str as u

#Create transactions table
def transform_transactions(data:pd.DataFrame) -> pd.DataFrame:
    transactions = data.copy()
    
    drop_columns = ['order_snapshot', 'payment_type', 'branch_name', 'order_snapshot_id']
    transactions.drop(columns = drop_columns, inplace=True)

    return transactions