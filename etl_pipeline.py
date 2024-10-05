
# Import Necessary Libraries
import pandas as pd
import  os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv

#Exrraction Layer
ziko_df = pd.read_csv(r'ziko_logistics_data.csv')

#Data cleaning and transformation
ziko_df.fillna({
    'Unit_price' : ziko_df['Unit_Price'].mean(),
    'Total_Cost' : ziko_df['Total_Cost'].mean(),
    'Discount_Rate' : 0.0,
    'Return_Reason': 'Unknown'
    }, inplace=True)


ziko_df['Date'] = pd.to_datetime(ziko_df['Date'])

# customer_df

customer_df = ziko_df[['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email',\
                       'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)

#Product_df

Product_df = ziko_df[['Product_ID', 'Quantity','Unit_Price', 'Product_List_Title' ]].copy().drop_duplicates().reset_index(drop=True)

ziko_logistics_df = ziko_df.merge(customer_df, on =['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email','Customer_Address'], how = 'left') \
                            .merge(Product_df, on =['Product_ID', 'Quantity','Unit_Price', 'Product_List_Title'], how = 'left') \
                            [['Transaction_ID', 'Date','Customer_ID', 'Product_ID', 'Total_Cost',\
                              'Discount_Rate', 'Sales_Channel','Order_Priority','Warehouse_Code',\
                              'Ship_Mode', 'Delivery_Status', 'Customer_Satisfaction', 'Item_Returned',\
                              'Return_Reason', 'Payment_Type', 'Taxable', 'Region','Country']].copy().drop_duplicates().reset_index(drop=True)

ziko_logistics_df['Date'] = ziko_logistics_df['Date'].astype('datetime64[us]')

# save the tables to cavs
customer_df.to_csv(r'data_set/customers.csv', index=False)
Product_df.to_csv(r'data_set/products.csv', index=False)
ziko_logistics_df.to_csv(r'data_set/ziko_logistics.csv', index=False)


print('Files has been loaded temporarily into local machine')


# data loading
# Azure blob connection

load_dotenv()

connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)


# create a function that would load the data into Azure blob storage as a parquer file

def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client =container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f'{blob_name} uploaded to Blob storage succesfully')


upload_df_to_blob_as_parquet(customer_df, container_client, 'rawdata/customer.parquet')
upload_df_to_blob_as_parquet(Product_df, container_client, 'rawdata/product.parquet')
upload_df_to_blob_as_parquet(ziko_logistics_df, container_client, 'rawdata/ziko_logistics_fact.parquet')