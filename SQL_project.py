#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np


# Plan of Action 
# 1. Clean data
# 2. Create temporary dataframes for normalized data and push them to SQL database.

# # Import data

# In[2]:


df = pd.read_csv('rolling_sales_sql.csv')
df.head()


# In[3]:


df.info()


# # Data Cleaning

# This our data cleaning process. Fristly, we dropped the columns that we do not need like easement, apartment number, and lot. Next we replaced the borough number with the actual name of the borough itself. The rest of the data cleaning was removing punctuations and filling in NA values as 0. Lastly, we renamed all the columns according to the column names we have created in our normalized tables. 

# In[4]:


# Remove Easement, APARTMENT NUMBER, LOT  Column 
df = df.drop('EASEMENT', 1)
df = df.drop('APARTMENT NUMBER', 1)
df = df.drop('LOT', 1)

# Replace Borough number with name
df.loc[df['BOROUGH'] == 1,'BOROUGH']='Manhattan'
df.loc[df['BOROUGH'] == 2,'BOROUGH']='Bronx'
df.loc[df['BOROUGH'] == 3,'BOROUGH']='Brooklyn'
df.loc[df['BOROUGH'] == 4,'BOROUGH']='Queens'
df.loc[df['BOROUGH'] == 5,'BOROUGH']='Staten Island'

# Remove ',' from numeric columns
df['LAND SQUARE FEET'] = df['LAND SQUARE FEET'].str.replace(',', '')
df['GROSS SQUARE FEET'] = df['GROSS SQUARE FEET'].str.replace(',', '')
df['SALE PRICE'] = df['SALE PRICE'].str.replace(',', '')
df['RESIDENTIAL UNITS'] = df['RESIDENTIAL UNITS'].str.replace(',', '')
df['COMMERCIAL UNITS'] = df['COMMERCIAL UNITS'].str.replace(',', '')
df['TOTAL UNITS'] = df['TOTAL UNITS'].str.replace(',', '')

# Replace missing values with 0 for units columns 
df['RESIDENTIAL UNITS'] = df['RESIDENTIAL UNITS'].fillna(0)
df['COMMERCIAL UNITS'] = df['COMMERCIAL UNITS'].fillna(0)
df['TOTAL UNITS'] = df['TOTAL UNITS'].fillna(0)

# Input missing sales prices as median sales price 
df['SALE PRICE'] = pd.to_numeric(df['SALE PRICE'])
df['SALE PRICE'] = df['SALE PRICE'].replace(0,df['SALE PRICE'].median())

# Convert SALE DATE column to date 
df['SALE DATE']= pd.to_datetime(df['SALE DATE'])


# Rename columns 
df = df.rename({'BOROUGH': 'borough_name', 'NEIGHBORHOOD': 'neighborhood', 
                'TAX CLASS AT PRESENT': 'tax_class_at_present', 'BLOCK': 'block',
                'BUILDING CLASS CATEGORY': 'building_class_category',
                'BUILDING CLASS AT PRESENT': 'building_class_at_present',
                'ADDRESS': 'address', 'ZIP CODE': 'zipcode',
                'RESIDENTIAL UNITS': 'residential_units', 'COMMERCIAL UNITS': 'commercial_units',
                'TOTAL UNITS': 'total_units', 'LAND SQUARE FEET': 'land_square_feet',
                'GROSS SQUARE FEET': 'gross_square_feet', 'YEAR BUILT': 'year',
                'TAX CLASS AT TIME OF SALE': 'tax_class_at_sale', 
                'BUILDING CLASS AT TIME OF SALE': 'building_class_at_sale',
                'SALE PRICE': 'sale_price', 'SALE DATE': 'sale_date'}, axis=1)


# Remove rows with no sq_ft and zipcode year
df_clean = df[df['land_square_feet'].notna()]
df_clean = df_clean[df_clean['zipcode'].notna()]
df_clean = df_clean[df_clean['year'].notna()]

# Remove decimals from year and zipcode
df_clean['year'] = df_clean['year'].astype(str).apply(lambda x: x.replace('.0',''))
df_clean['zipcode'] = df_clean['zipcode'].astype(str).apply(lambda x: x.replace('.0',''))
df_clean


# In[5]:


df_clean.info()


# # Connect to DB and create tables 

# In[6]:


#connect to database 
conn_url = 'postgresql://postgres:1234567@localhost/5310_project'
engine = create_engine(conn_url)
connection = engine.connect()

# Pass the SQL statements that create all tables
stmt = """
    CREATE TABLE Borough(
        borough_id char(5),
        borough_name varchar(20) NOT NULL,
        PRIMARY KEY (borough_id)
); 

   CREATE TABLE Neighborhood (
        neighborhood_id char(5),
        neighborhood char(50) NOT NULL,
        PRIMARY KEY (neighborhood_id)
);

    CREATE TABLE Tax_Class(
        tax_class_id char (5),
        tax_class_at_present char (5) NOT NULL,
        tax_class_at_sale int NOT NULL,
        PRIMARY KEY (tax_class_id)
);
    CREATE TABLE Category_Tax_Class(
        category_tax_id char(5),
        building_class_category varchar(50) NOT NULL,
        tax_class_id char (5),
        PRIMARY KEY (category_tax_id),
        FOREIGN KEY (tax_class_id) REFERENCES Tax_Class

);

    CREATE TABLE Block_Zip(
        block char (5),
        zipcode char(5) NOT NULL,
        PRIMARY KEY (block)
);

    CREATE TABLE Size (
        size_id char (6),
        land_square_feet int,
        gross_square_feet int,
        PRIMARY KEY (size_id)
);

    CREATE TABLE Unit (
        unit_id char (5),
        residential_units int,
        commercial_units int,
        total_units int,
        PRIMARY KEY (unit_id)
);

    CREATE TABLE Building_Info(
        sale_id char(6),
        borough_id char (5),
        neighborhood_id char(5),
        category_tax_id char(5),
        block char(5),
        size_id char(6),
        unit_id char(5),
        sale_price integer,
        sale_date date,
        year int,
        address varchar(120),
        building_class_at_present varchar(10),
        building_class_at_sale varchar(10),
        PRIMARY KEY (sale_id),
        FOREIGN KEY (borough_id) REFERENCES Borough,
        FOREIGN KEY (neighborhood_id) REFERENCES Neighborhood,
        FOREIGN KEY (category_tax_id) REFERENCES Category_Tax_Class,
        FOREIGN KEY (block) REFERENCES Block_Zip,
        FOREIGN KEY (size_id) REFERENCES Size,
        FOREIGN KEY (unit_id) REFERENCES Unit

);
    
"""

# Execute the statement to create tables
connection.execute(stmt)


# # Populating the database

# This section shows the code for populating the database in PgAdmin. Temporary dataframes were created for normalized tables and ids were added to each dataframe accordingly. Each of the dataframes were then pushed to the database. 

# In[7]:


#creating transaction_id for all transactions 
df_clean.insert(0, 'sale_id', range(1, 1 + len(df_clean)))


# In[8]:


#Create neighborhood table, drop duplicates to assign neighborhood_id 
neighborhood_df = df_clean[['neighborhood']]
neighborhood_df = neighborhood_df.drop_duplicates()
neighborhood_df.insert(0, 'neighborhood_id', range(1, 1 + len(neighborhood_df)))

#Push table to the database 
neighborhood_df.to_sql(name='neighborhood', con=engine, if_exists='append', index=False)


# Map back to clean dataframe
neighborhood_list = [neighborhood_df.neighborhood_id[neighborhood_df.neighborhood == i].values[0] for i in df_clean.neighborhood]
df_clean.insert(1,'neighborhood_id', neighborhood_list)
df_clean


# In[9]:


#Create borough table, drop duplicates to assign neighborhood_id 
borough_df = df_clean[['borough_name']]
borough_df = borough_df.drop_duplicates()
borough_df.insert(0, 'borough_id', range(1, 1 + len(borough_df)))

#Push table to the database 
borough_df.to_sql(name='borough', con=engine, if_exists='append', index=False)

# Map back to clean dataframe
borough_list = [borough_df.borough_id[borough_df.borough_name == i].values[0] for i in df_clean.borough_name]
df_clean.insert(1,'borough_id', borough_list)
df_clean


# In[10]:


#Create unit id, create unit table and push to database
df_clean.insert(0, 'unit_id', range(1, 1 + len(df_clean)))
unit_df1 = df_clean[['unit_id','residential_units', 'commercial_units', 'total_units']]


#Push table to the database 
unit_df1.to_sql(name='unit', con=engine, if_exists='append', index=False)


# In[11]:


# Create block_zip 
block_zip = df_clean[['block','zipcode']]

# Remove Duplicates
block_zip = block_zip.drop_duplicates(subset='block', keep="first")

# Push to database
block_zip.to_sql(name='block_zip', con=engine, if_exists='append', index=False)


# In[12]:


# Create size table, drop duplicates to assign neighborhood_id 
size_df = df_clean[['land_square_feet','gross_square_feet']]
size_df = size_df.drop_duplicates()
size_df.insert(0, 'size_id', range(1, 1 + len(size_df)))

# Push to database 
size_df.to_sql(name='size', con=engine, if_exists='append', index=False)

# Map back to clean dataframe
size_list = [size_df.size_id[size_df.land_square_feet == i].values[0] for i in df_clean.land_square_feet]
df_clean.insert(1,'size_id', size_list)
df_clean


# In[13]:


# Create category tax table, drop duplicates to assign tax id 
tax_df = df_clean[['tax_class_at_present','tax_class_at_sale']]
tax_df = tax_df.drop_duplicates()
tax_df.insert(0, 'tax_class_id', range(1, 1 + len(tax_df)))

# Push to df
tax_df.to_sql(name='tax_class', con=engine, if_exists='append', index=False)


# In[14]:


# Map back to dataframe 
def myfunc(tax_class_at_present, tax_class_at_sale):
    if tax_class_at_present == '1' and tax_class_at_sale ==1:
        tax_id=1
    elif tax_class_at_present == '2B' and tax_class_at_sale ==2:
        tax_id=2
    elif tax_class_at_present == '2' and tax_class_at_sale ==2:
        tax_id=3
    elif tax_class_at_present == '4' and tax_class_at_sale ==4:
        tax_id=4
    elif tax_class_at_present == '2A' and tax_class_at_sale ==2:
        tax_id=5
    elif tax_class_at_present == '1B' and tax_class_at_sale ==1:
        tax_id=6
    elif tax_class_at_present == '1B' and tax_class_at_sale ==4:
        tax_id=7
    elif tax_class_at_present == '2' and tax_class_at_sale ==1:
        tax_id=8
    elif tax_class_at_present == '2' and tax_class_at_sale ==4:
        tax_id=9
    elif tax_class_at_present == '1' and tax_class_at_sale ==2:
        tax_id=10
    elif tax_class_at_present == '2A' and tax_class_at_sale ==1:
        tax_id=11
    elif tax_class_at_present == '1' and tax_class_at_sale ==4:
        tax_id=12
    elif tax_class_at_present == '4' and tax_class_at_sale ==1:
        tax_id=13
    elif tax_class_at_present == '2B' and tax_class_at_sale ==1:
        tax_id=14
    return tax_id

df_clean['tax_class_id'] = df_clean.apply(lambda x: myfunc(x['tax_class_at_present'], x['tax_class_at_sale']), axis=1)


# In[15]:


# Create building table, drop duplicates to assign category tax id 

cat_df = df_clean[['building_class_category']]
cat_df = cat_df.drop_duplicates()
cat_df.insert(0, 'category_tax_id', range(1, 1 + len(cat_df)))
cat_df.head()

cat_list = [cat_df.category_tax_id[cat_df.building_class_category == i].values[0] for i in df_clean.building_class_category]
df_clean.insert(1,'category_tax_id', cat_list)

df_clean

#Push to database
#cat_df.to_sql(name='category_tax_class', con=engine, if_exists='append', index=False)


# In[16]:


cat_df_1 = df_clean[['category_tax_id','building_class_category','tax_class_id']]
cat_df_1 = cat_df_1.drop_duplicates(subset='category_tax_id', keep="first")

cat_df_1
#Push to database
cat_df_1.to_sql(name='category_tax_class', con=engine, if_exists='append', index=False)


# In[17]:


# Creating building info table 

building_df = df_clean[['sale_id','borough_id','neighborhood_id', 'category_tax_id', 'block', 
                       'size_id', 'unit_id', 'sale_price', 'sale_date', 'year', 'address',
                       'building_class_at_present', 'building_class_at_sale']]

#Push to database
building_df.to_sql(name='building_info', con=engine, if_exists='append', index=False)

