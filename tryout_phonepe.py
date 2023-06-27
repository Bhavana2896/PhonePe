import os
from git.repo.base import Repo
import json
import pandas as pd
import psycopg2
from SQL import push_data_into_mysql

##### cloning repository-----------#
repository_url = 'https://github.com/PhonePe/pulse.git'
destination_path = '/Users/bhavana/Studies/pulse'

# Clone the repository
#import subprocess
#subprocess.run(['git', 'clone', repository_url, destination_path],check=True)

#or the below method
reposit_url='https://github.com/PhonePe/pulse.git'
clone_path='/Users/bhavana/Studies'
#creates path if it doesn't exist
if not os.path.exists(clone_path):
    os.makedirs(clone_path)
#clone path inside rootpath
#clone the repository to the desired path
try:
    directory = os.path.join(clone_path, os.path.basename(repository_url).removesuffix('.git'))
    Repo.clone_from(repository_url, directory)
    print('Repository cloned successfully.')
except Exception as e:
    print(f'Non empty directory exists: {str(e)}')

#-------file processing-----------#
def process_directory(path):
    for item in os.listdir(path):
        item_path = os.path.join(path, item)

        #recursive function for directories till it reaches a file to process
        if os.path.isdir(item_path):
            process_directory(item_path)

        #processing of file in directory
        if os.path.isfile(item_path) and item.endswith('.json'):
            process_json_file(item_path)

def process_json_file(file_path):
    with open(file_path) as file:
        data = json.load(file)
        df = pd.DataFrame(data)
    return df
        # Convert JSON data to DataFrame
        # df = pd.DataFrame(data)
        # print("File Path:", file_path)
        # print("DataFrame:")
        # print(df)
        # print()

root_path = "/Users/bhavana/Studies"  # Replace with the actual path to your root directory
process_directory(root_path)

# Function to extract all paths that has sub-directory in the name of 'state'
# This replacement is necessary because in some operating systems, such as Windows,
# paths are represented using backslashes, while in Python, forward slashes are typically used.
def extract_paths(directory):
    path_list = []
    for root, dirs, files in os.walk(directory):
        if os.path.basename(root) == 'state':
            path_list.append(root.replace('\\', '/'))
            #path_list.append(root)(enough for me but above is required for generalized usage)
    return path_list


state_directories = extract_paths(directory)
print(state_directories)

# --------1. Aggregate Transaction-----#
state_path = state_directories[4]
state_list = os.listdir(state_path)
agg_trans_dict = {
                  'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],
                  'Transaction_count': [], 'Transaction_amount': []
                  }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for transaction_data in df['data']['transactionData']:

                    type = transaction_data['name']
                    count = transaction_data['paymentInstruments'][0]['count']
                    amount = transaction_data['paymentInstruments'][0]['amount']

                    # Appending to agg_trans_dict

                    agg_trans_dict['State'].append(state)
                    agg_trans_dict['Year'].append(year)
                    agg_trans_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    agg_trans_dict['Transaction_type'].append(type)
                    agg_trans_dict['Transaction_count'].append(count)
                    agg_trans_dict['Transaction_amount'].append(amount)
            except:
                pass

agg_trans_df = pd.DataFrame(agg_trans_dict)
#agg_trans_df['Year'] = pd.to_datetime(agg_trans_df['Year'])

# print('agg_trans_df')
# print(agg_trans_df.isnull().sum())
# agg_trans_df = agg_trans_df.drop_duplicates()
# #print(agg_trans_df)
# print()

#-------2. Aggregate User-----#
state_path = state_directories[5]
state_list = os.listdir(state_path)
agg_user_dict = {
                 'State': [], 'Year': [], 'Quarter': [], 'Brand': [],
                 'Transaction_count': [], 'Percentage': []
                 }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for user_data in df['data']['usersByDevice']:

                    brand = user_data['brand']
                    count = user_data['count']
                    percent = user_data['percentage']

                    # Appending to agg_user_dict

                    agg_user_dict['State'].append(state)
                    agg_user_dict['Year'].append(year)
                    agg_user_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    agg_user_dict['Brand'].append(brand)
                    agg_user_dict['Transaction_count'].append(count)
                    agg_user_dict['Percentage'].append(percent)
            except:
                pass

agg_user_df = pd.DataFrame(agg_user_dict)
#agg_user_df['Year'] = pd.to_datetime(agg_user_df['Year'])

# print('agg_user_df')
# agg_user_df.isnull().sum()
# agg_user_df = agg_user_df.drop_duplicates()
# #print(agg_user_df)
# print()

#-------3. Map Transaction---------#
state_path = state_directories[2]
state_list = os.listdir(state_path)
map_trans_dict = {
                    'State': [], 'Year': [], 'Quarter': [], 'District': [],
                    'Transaction_count': [], 'Transaction_amount': []
                    }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for transaction_data in df['data']['hoverDataList']:

                    district = transaction_data['name']
                    count = transaction_data['metric'][0]['count']
                    amount = transaction_data['metric'][0]['amount']

                    # Appending to map_trans_dict

                    map_trans_dict['State'].append(state)
                    map_trans_dict['Year'].append(year)
                    map_trans_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    map_trans_dict['District'].append(district.removesuffix(' district').title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    map_trans_dict['Transaction_count'].append(count)
                    map_trans_dict['Transaction_amount'].append(amount)
            except:
                pass

map_trans_df = pd.DataFrame(map_trans_dict)
#map_trans_df['Year'] = pd.to_datetime(map_trans_df['Year'])

# print('map_trans_df')
# map_trans_df.isnull().sum()
# map_trans_df = map_trans_df.drop_duplicates()
# #print(map_trans_df)
# print()

#------------4. Map User-------#
state_path = state_directories[3]
state_list = os.listdir(state_path)
map_user_dict = {
                 'State': [], 'Year': [], 'Quarter': [], 'District': [],
                 'Registered_users': [], 'App_opens': []
                 }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district, user_data in df['data']['hoverData'].items():

                    reg_user_count = user_data['registeredUsers']
                    app_open_count = user_data['appOpens']

                    # Appending to map_user_dict

                    map_user_dict['State'].append(state)
                    map_user_dict['Year'].append(year)
                    map_user_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    map_user_dict['District'].append(district.removesuffix(' district').title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    map_user_dict['Registered_users'].append(reg_user_count)
                    map_user_dict['App_opens'].append(app_open_count)
            except:
                pass

map_user_df = pd.DataFrame(map_user_dict)
#map_user_df['Year'] = pd.to_datetime(map_user_df['Year'])

# print('map_user_df')
# map_user_df.isnull().sum()
# map_user_df = map_user_df.drop_duplicates()
# #print(map_user_df)
# print()

#-------------5. Top Transaction District-wise--------#
state_path = state_directories[0]
state_list = os.listdir(state_path)
top_trans_dist_dict = {
                        'State': [], 'Year': [], 'Quarter': [], 'District': [],
                        'Transaction_count': [], 'Transaction_amount': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district_data in df['data']['districts']:

                    name = district_data['entityName']
                    count = district_data['metric']['count']
                    amount = district_data['metric']['amount']

                    # Appending to top_trans_dist_dict

                    top_trans_dist_dict['State'].append(state)
                    top_trans_dist_dict['Year'].append(year)
                    top_trans_dist_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    top_trans_dist_dict['District'].append(name.title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    top_trans_dist_dict['Transaction_count'].append(count)
                    top_trans_dist_dict['Transaction_amount'].append(amount)
            except:
                pass

top_trans_dist_df = pd.DataFrame(top_trans_dist_dict)
#top_trans_dist_df['Year'] = pd.to_datetime(top_trans_dist_df['Year'])

# print('top_trans_dist_df')
# top_trans_dist_df.isnull().sum()
# top_trans_dist_df = top_trans_dist_df.drop_duplicates()
# #print(top_trans_dist_df)
# print()

#-----------6. Top Transaction Pincode-wise----#
state_path = state_directories[0]
state_list = os.listdir(state_path)
top_trans_pin_dict = {
                        'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
                        'Transaction_count': [], 'Transaction_amount': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for regional_data in df['data']['pincodes']:

                    name = regional_data['entityName']
                    count = regional_data['metric']['count']
                    amount = regional_data['metric']['amount']

                    # Appending to top_trans_pin_dict

                    top_trans_pin_dict['State'].append(state)
                    top_trans_pin_dict['Year'].append(year)
                    top_trans_pin_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    top_trans_pin_dict['Pincode'].append(name)
                    top_trans_pin_dict['Transaction_count'].append(count)
                    top_trans_pin_dict['Transaction_amount'].append(amount)
            except:
                pass

top_trans_pin_df = pd.DataFrame(top_trans_pin_dict)
# Fill null values in the "Pincode" column with a specified value
# Fill possible pincode values with reference to data
top_trans_pin_df['Pincode'].fillna('194105', inplace=True)
top_trans_pin_df.isnull().sum()

# print('top_trans_dist_df')
# top_trans_pin_df.isnull().sum()
# top_trans_pin_df = top_trans_pin_df.drop_duplicates()
# #print(top_trans_pin_df)
# print()

#-------7. Top User District-wise--------#
state_path = state_directories[1]
state_list = os.listdir(state_path)
top_user_dist_dict = {
                        'State': [], 'Year': [], 'Quarter': [],
                        'District': [], 'Registered_users': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district_data in df['data']['districts']:

                    name = district_data['name']
                    count = district_data['registeredUsers']

                    # Appending to top_user_dist_dict

                    top_user_dist_dict['State'].append(state)
                    top_user_dist_dict['Year'].append(year)
                    top_user_dist_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    top_user_dist_dict['District'].append(name.title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    top_user_dist_dict['Registered_users'].append(count)
            except:
                pass

top_user_dist_df = pd.DataFrame(top_user_dist_dict)
#top_user_dist_df['Year'] = pd.to_datetime(top_user_dist_df['Year'])

# print('top_user_dist_df')
# top_user_dist_df.isnull().sum()
# top_user_dist_df = top_user_dist_df.drop_duplicates()
# #print(top_user_dist_df)
# print()

#--------8. Top User Pincode-wise---------#
state_path = state_directories[1]
state_list = os.listdir(state_path)
top_user_pin_dict = {
                        'State': [], 'Year': [], 'Quarter': [],
                        'Pincode': [], 'Registered_users': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for regional_data in df['data']['pincodes']:

                    name = regional_data['name']
                    count = regional_data['registeredUsers']

                    # Appending to top_user_pin_dict

                    top_user_pin_dict['State'].append(state)
                    top_user_pin_dict['Year'].append(year)
                    top_user_pin_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    top_user_pin_dict['Pincode'].append(name)
                    top_user_pin_dict['Registered_users'].append(count)
            except:
                pass

top_user_pin_df = pd.DataFrame(top_user_pin_dict)
#top_user_pin_df['Year'] = pd.to_datetime(top_user_pin_df['Year'])

# print('top_user_pin_df')
# top_user_pin_df.isnull().sum()
# top_user_pin_df = top_user_pin_df.drop_duplicates()
# #print(top_user_pin_df)
# print()

#-----List of dataframes created-------#
df_list = [df for df in globals() if isinstance(globals()[df], pd.core.frame.DataFrame) and df.endswith('_df')]
print(df_list)

#------column-wise null & duplicate values----#
for df_name in df_list:
    df = globals()[df_name]
    print(f"{df_name}:")
    print(f"Null count: \n{df.isnull().sum()}")
    print(f"Duplicated rows count: \n{df.duplicated().sum()}")
    print(df.shape)
    print("\n", 25 * "_", "\n")

#--------dataframe information----#
for df_name in df_list:
    df = globals()[df_name]
    print(df_name + ':\n')
    df.info()
    print("\n", 45 * "_", "\n")

#----------creation of csv files for dataframes---#
def save_dfs_as_csv(df_list):
    subfolder = 'Miscellaneous'
    if not os.path.exists(subfolder):
        os.makedirs(subfolder)

    for df_name in df_list:
        df = globals()[df_name]
        file_path = os.path.join(subfolder, df_name.replace('_df', '') + '.csv')
        df.to_csv(file_path, index=False)


# Calling function to execute

save_dfs_as_csv(df_list)

#------------SQL SECTION----------#
conn = psycopg2.connect(host='localhost',
                                       port=5432,
                                       database='capstone2',
                                       user='postgres',
                                       password='Girish47$')
cursor = conn.cursor()

dfs = {
    'agg_trans': agg_trans_df,
    'agg_user': agg_user_df,
    'map_trans': map_trans_df,
    'map_user': map_user_df,
    'top_trans_dist': top_trans_dist_df,
    'top_trans_pin': top_trans_pin_df,
    'top_user_dist': top_user_dist_df,
    'top_user_pin': top_user_pin_df
}

# Mapping table name to associated columns for each table

table_columns = {
    'agg_trans': list(agg_trans_df.columns),
    'agg_user': list(agg_user_df.columns),
    'map_trans': list(map_trans_df.columns),
    'map_user': list(map_user_df.columns),
    'top_trans_dist': list(top_trans_dist_df.columns),
    'top_trans_pin': list(top_trans_pin_df.columns),
    'top_user_dist': list(top_user_dist_df.columns),
    'top_user_pin': list(top_user_pin_df.columns)
}

push_data_into_mysql(conn, cursor, dfs, table_columns)

# Get list of tables in database

cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'public'")
tables = cursor.fetchall()

# Loop through tables and get count of rows and columns in MySQL

for table in tables:
    table_catalog = table[0]
    table_schema = table[1]
    table_name = table[2]
    cursor.execute(f"SELECT COUNT(*) FROM {table_catalog}.{table_schema}.{table_name}")
    row_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_catalog='{table_catalog}' AND table_schema='{table_schema}' AND table_name='{table_name}'")
    column_count = cursor.fetchone()[0]

    # Check if shape of DataFrame matches count of rows and columns in table

    df = dfs[table_name]
    if df.shape == (row_count, column_count):
        print(f"{table_name} table has {row_count} rows and {column_count} columns and shape matches DataFrame.")
    else:
        print(f"{table_name} table has {row_count} rows and {column_count} columns but shape does not match DataFrame.")

cursor.close()
conn.close()



