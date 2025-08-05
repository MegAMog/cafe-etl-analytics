import pandas as pd
import utils.uuid_str as u

#Create transactions table
def transform_order_snapshots(data:pd.DataFrame) -> pd.DataFrame:
    #1. Get only necessary columns for order snapshots
    order_snapshots = data.copy()

    drop_columns = ['order_date', 'branch_id', 'bill', 'payment_type', 'payment_type_id']
    order_snapshots.drop(drop_columns, inplace=True, axis=1)

    #2. Expand order_snapshot with comma-separated values into multiple rows
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html
    order_snapshots ['order_snapshot'] = order_snapshots['order_snapshot'].str.split(', ')
    order_snapshots = order_snapshots.explode('order_snapshot')

    #3. Split order_snapshot into product_name and price
    order_snapshots [['product_name', 'price']] = order_snapshots['order_snapshot'].str.rsplit(' - ', n=1, expand=True)

    #4. Convert price to float
    order_snapshots ['price'] = pd.to_numeric(order_snapshots['price'], errors='coerce')
    
    #5. Add new column product_id with UUID based on branch_name, product_name, price
    # - axis=1 -> apply to each row
    order_snapshots['product_id'] = order_snapshots.apply( 
        lambda row: u.create_uuid_from_list([row['branch_name'], row['product_name'], str(row['price'])]), 
        axis=1)

    #6. Create products table
    products = order_snapshots.copy()
    drop_columns=['branch_name', 'order_snapshot', 'order_id', 'order_snapshot_id']
    products.drop(drop_columns, inplace=True, axis=1)

    #7. Remove duplicates
    products.drop_duplicates(inplace = True)


    drop_columns = ['branch_name', 'order_snapshot', 'product_name', 'price']
    order_snapshots.drop(drop_columns, inplace=True, axis=1)

    order_snapshots = order_snapshots.groupby(['order_id','order_snapshot_id','product_id']).size().reset_index(name='quantity')
    # order_snapshots = order_snapshots.sort_values(by='quantity', ascending=False)

    return  products, order_snapshots

