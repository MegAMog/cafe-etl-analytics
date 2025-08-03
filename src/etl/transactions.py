import pandas as pd
import utils.uuid as u

#Create transactions table
def transform_transactions(data:pd.DataFrame, branches:pd.DataFrame, payment_types:pd.DataFrame) -> pd.DataFrame:
    #1. Get only necessary columns for transactions
    transactions = data.copy()

    #2. Add transaction_id 
    transactions['order_id'] = transactions.apply(
    lambda row: u.create_uuid_from_list(
        [
            str(row['order_date']), 
            row['branch_name'],
            row['order_snapshot'],
            str(row['bill']),
            row['payment_type']
        ]
    ),
    axis=1)

    #3. Drop order snapshot
    transactions.drop(columns = 'order_snapshot', inplace=True)

    #4. Replace foreign key columns in with corresponding UUIDs
    #https://pandas.pydata.org/docs/user_guide/merging.html
    transactions = transactions.merge(branches[['branch_name', 'branch_id']], on='branch_name', how='left')
    transactions.drop(columns=['branch_name'], inplace=True)

    transactions = transactions.merge(payment_types[['payment_type', 'payment_type_id']], on='payment_type', how='left')
    transactions.drop(columns=['payment_type'], inplace=True)


    return transactions