# %%
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
import pandas as pd
pd.set_option('mode.chained_assignment', None)

# %%
import pandas as pd
pd.options.display.max_columns = 0

# %%

# Load the credentials from the JSON file (replace with the path to your file)
sf_path = r'inductive-gift-355101-48518c54d576.json'
gc = pygsheets.authorize(service_account_file=sf_path)


# %%
def get_number_gsheet(service_file_path, spreadsheet_id, sheet_name):
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    wks = sh.worksheet_by_title(sheet_name)
    df = wks.get_as_df(
        numerize=False, value_render='UNFORMATTED_VALUE', empty_value=None)
    df = df.replace(to_replace='', value=None)

    return df

# %%
# lay du lieu tu googhesheet ma khong co du lieu so


def get_df_from_gsheet(service_file_path, spreadsheet_id, sheet_name, start=None):
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    wks = sh.worksheet_by_title(sheet_name)
    df = wks.get_as_df(start, numerize=False, empty_value=None)
    df = df.replace(to_replace='', value=None)
    df.drop_duplicates(inplace=True)

    return df

# %%
def get_data(sheet_name, start):
    spreadsheet_id = '1Bmm9GGP3QE5k_T8eODN3Hrh8ICzMgAazYkw0gWNRZDs'

    gc = pygsheets.authorize(service_file=sf_path)
    sh = gc.open_by_key(spreadsheet_id)
    wks = sh.worksheet_by_title(sheet_name)
    df = wks.get_as_df(start, numerize=False, value_render='UNFORMATTED_VALUE', empty_value=None)
    df = df.replace(to_replace='', value=None)
    # df.drop_duplicates(inplace=True)

    df2 = df.transpose()
    df2.iloc[:,1] = df2.iloc[:,1].ffill()
    df3 = df2.transpose()
    # Combine the two lists
    combined_list = [f"{a}|{b}" for a, b in zip(df3.iloc[1].to_list(), df3.iloc[2].to_list())]
    df3.columns = combined_list
    return df3

# %%
def extract_daily(df3, sheet_name):
    df_daily = df3.iloc[10:,:]
    df_daily2 = df_daily.melt(id_vars='None|None')
    df_daily2[['user', 'criteria']] =df_daily2['variable'].str.split('|', expand=True)
    df_daily2.rename(columns={'None|None':'report_date'}, inplace=True)
    df_daily2.drop(columns='variable', inplace=True)
    df_daily2.dropna(subset='value', inplace=True)
    df_daily3 = df_daily2.query("value!=0").pivot(columns='criteria', index=['user', 'report_date'], values='value').reset_index()
    df_daily3['report_date'] = df_daily3['report_date'].astype('str') + '/' + sheet_name
    df_daily3['report_date'] = pd.to_datetime(df_daily3['report_date'], format='%d/%m/%Y')
    # df_daily3.to_parquet(rf".\data\daily\{str(sheet_name).replace('/', '-')}.parquet", index=False )
    return df_daily3

# %%
def extract_target(df3, sheet_name):
    df_target = df3.iloc[3:10]
    df_target.rename(columns={"None|None":"item"}, inplace=True)
    df_target.query("item.isin(['Target', 'Daily'])", inplace=True)

    df_target2 = df_target.melt(id_vars='item')
    df_target2[['user', 'criteria']] =df_target2['variable'].str.split('|', expand=True)
    # df_target2.rename(columns={'item':'report_date'}, inplace=True)
    df_target2.drop(columns='variable', inplace=True)
    df_target2.dropna(subset='value', inplace=True)

    df_target2.drop_duplicates(subset=['item', 'user'], inplace=True)
    df_target3 = df_target2.query("value!=0").pivot(columns='item', index=['user'], values='value').reset_index()

    df_target3['report_month'] = '01/' + sheet_name
    df_target3['report_month'] = pd.to_datetime(df_target3['report_month'], format='%d/%m/%Y')
    # df_target3.to_parquet(rf".\data\target\{str(sheet_name).replace('/', '-')}.parquet", index=False )
    return df_target3

# %%
sheets = [
        '9/2024',
        # '8/2024',
        # '7/2024',
        # '6/2024',
        # '5/2024',
        # '4/2024',
        # '3/2024',
        # '2/2024',
        # '1/2024',
        # '12/2023',
        # '11/2023',
        # '10/2023',
        # '9/2023',
        # '8/2023',
        # '7/2023',
        # '6/2023',
        # '5/2023',
        # '4/2023',
        # '3/2023',
        # '2/2023',
        # '1/2023',
]
for sheet_name in sheets:
    df3 = get_data(sheet_name=sheet_name, start='A1')
    df_daily = extract_daily(df3, sheet_name)
    df_target = extract_target(df3, sheet_name)
    # Get start of month and number of days in the month
    start_of_month = df_target.loc[0,'report_month']
    num_days = start_of_month.days_in_month

    # Create a date range for each day of the month
    date_range = pd.date_range(start_of_month, periods=num_days, freq='D')
    df_target['report_date'] = [date_range]*len(df_target)
    df_target = df_target.explode('report_date')
    df_target['report_day'] = df_target['report_date'].dt.day
    df_target['no_of_day'] = num_days
    df_target['Daily'] = df_target['Daily'].astype(int)

    df_target['Target'] = df_target['Target'].astype(int)
    df_target['daily_target_norm'] =  df_target['Target']/df_target["no_of_day"]
    df_target['mtd_target_norm'] = df_target['daily_target_norm'] * df_target['report_day']
    dta = df_target.merge(df_daily, how='left', on=['user','report_date'])
    dta['Total'] = dta['Total'].fillna(0)
    dta['Total'] = pd.to_numeric(dta['Total'], errors='coerce')
    dta['mtd_actual'] = dta.sort_values(by=['user', 'report_date']).groupby(['user'])['Total'].cumsum()
    # danh dau ngay dai daily target
    dta['flg_daily']  = 0
    flt = dta['Total'] >= dta['Daily']
    dta.loc[flt, 'flg_daily'] = 1

    # danh dau ngay co tap luyen
    dta['flg_workout'] = 0
    flt = dta['Total'] >0
    dta.loc[flt, 'flg_workout'] = 1


    for col in ['Burpee', 'Core','Pushup', 'Run', 'Squat','flg_daily', 'flg_workout']:
        try:
            dta[col] = pd.to_numeric(dta[col], errors='coerce')
            dta[col] = dta[col].fillna(0)
            new_col = 'mtd_'+col
            dta[new_col] = dta.sort_values(by=['user', 'report_date']).groupby(['user'])[col].cumsum()
        except Exception as e:
            print(e)
    # tinh toan accumulate
    dta.to_parquet(rf"data/daily/dta_{str(sheet_name).replace('/', '-')}.parquet", index=False )
    print(sheet_name)


import subprocess

# Function to run a git command
def run_git_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Command succeeded: {command}")
    else:
        print(f"Command failed: {command}\nError: {result.stderr}")

# Example usage

# 1. Add changes (stage files)
run_git_command("git add .")

# 2. Commit the changes
commit_message = "Update data"
run_git_command(f'git commit -m "{commit_message}"')

# 3. Push the changes
# branch_name = "main"  # Replace with your branch name
# run_git_command(f"git push origin {branch_name}")
run_git_command(f"git push")

