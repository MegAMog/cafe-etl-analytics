import pandas as pd
import utils.uuid as u

#Create products table
def transfrom_products(data:pd.DataFrame) -> pd.DataFrame:
    #1. Get only necessary columns for products
    products_raw = data[['branch_name', 'order_snapshot']].copy()

    #2. Split order_snapshot with comma-separated values into multiple rows, not columns == "exploding" the column
    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html

    products_raw ['items'] = products_raw['order_snapshot'].str.split(', ')
    products_raw = products_raw.explode('items')

    #3. Split items into product_name and price
    products_raw [['product_name', 'price']] = products_raw['items'].str.rsplit(' - ', n=1, expand=True)

    #4. Convert price to float
    products_raw ['price'] = pd.to_numeric(products_raw['price'], errors='coerce')

    #5. Drop order_snapshot and items
    products  = products_raw.drop(columns = ['order_snapshot', 'items'])

    #6. Remove duplicates
    products.drop_duplicates(inplace = True)

    #7. Add new column product_id with UUID based on branch_name, product_name, price
    # - axis=1 -> apply to each row
    products['product_id'] = products.apply( 
        lambda row: u.create_uuid_from_list([row['branch_name'], row['product_name'], str(row['price'])]), 
        axis=1)
    
    #8. Drop branch name
    products.drop(columns = ['branch_name'], inplace = True)

    return products