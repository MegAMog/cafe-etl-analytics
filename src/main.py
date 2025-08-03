import pandas as pd
import os
import utils.path as path


#Specifie files
csv_file = os.path.join(path.data_dir, "leeds_09-05-2023_09-00-00_done.csv")

#CSV-files don't have headers. Create column_names list of headers.
column_names = ['order_date', 'branch_name', 'customer', 'order_snapshot', 'bill', 'payment_type', 'card_number']


#Step 1: extract data from csv file into pandas DataFrame object
raw_data = pd.read_csv(csv_file, names = column_names)

#Print info about data in csv file(DataFrame object)
print(raw_data.info())

# #Retrieve first 10 rows and print int out
# limit_10 = raw_data.loc[0:9]
# print(limit_10) 

# #Retrieve header and first 10 rows and print int out -> use head() 
# #Note: to see header and last 10 rows - use tail()
print(raw_data.head(10))
