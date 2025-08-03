import pandas as pd
import os
import utils.path as path
import utils.uuid as u
import etl.branches as b
import etl.payment_types as pt
import etl.transactions as t
import etl.order_snapshots as o

#Specifie files
csv_file = os.path.join(path.data_dir, "leeds_09-05-2023_09-00-00_done.csv")

#CSV-files don't have headers. Create column_names list of headers.
column_names = ['order_date', 'branch_name', 'customer_name', 'order_snapshot', 'bill', 'payment_type', 'customer_card_number']


#Step 1: extract data from csv file into pandas DataFrame object
raw_data = pd.read_csv(csv_file, names = column_names)

#Print info about data in csv file(DataFrame object)
# print(raw_data.info())

# #Retrieve f35713571irst 10 rows and print int out
# limit_10 = raw_data.loc[0:9]
# print(limit_10) 

# #Retrieve header and first 10 rows and print int out -> use head() 
# #Note: to see header and last 10 rows - use tail()
# print(raw_data.head(10))


#Step 2: remove PII - customer name and card number
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
pii_column_names = ['customer_name', 'customer_card_number']
raw_data = raw_data.drop(columns = pii_column_names)
# print(raw_data_without_pii.info())
# print(raw_data_without_pii)


#Step 3: normalize data to fit DB Schema + added UUID
cleaned_data = raw_data.copy()

#3.1 Convert into a correct format
#-order_date should be string that represents a date -> to_datetime()
cleaned_data ['order_date'] = pd.to_datetime(cleaned_data['order_date'], format='mixed')
# print(cleaned_data.info())

#-bill should be float
#errors='coerce' -> replace that value with NaN
cleaned_data ['bill'] = pd.to_numeric(cleaned_data['bill'], errors='coerce')

#3.2 Drop NULL/empty cells in place
cleaned_data.dropna(inplace = True)

#3.3 Added UUIDs
cleaned_data['order_id'] = cleaned_data.apply(
    lambda row: u.create_uuid_from_list(
        [
            str(row['order_date']), 
            row['branch_name'],
            row['order_snapshot'],
            str(row['bill']),
            row['payment_type']
        ]
    ), axis=1)

cleaned_data['order_snapshot_id'] = cleaned_data.apply(
    lambda row: u.create_uuid_from_list(
        [
            row['branch_name'],
            row['order_snapshot']
        ]
    ), axis=1)

cleaned_data['branch_id'] = cleaned_data.apply(
    lambda row: u.create_uuid_from_list(
        [
            row['branch_name']
        ]
    ), axis=1)

cleaned_data['payment_type_id'] = cleaned_data.apply(
    lambda row: u.create_uuid_from_list(
        [
            row['payment_type']
        ]
    ), axis=1)


#-create branch table
branch = b.transform_branch(cleaned_data)

#-create payment_types
payment_types = pt.transform_payment_types(cleaned_data)

#-create transactions
transactions = t.transform_transactions(cleaned_data)

#-create order_snapshot and products
products, order_snapshots = o.transform_order_snapshots(cleaned_data)
