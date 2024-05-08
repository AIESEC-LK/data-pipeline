
import asyncio
import aiohttp
import pandas as pd
import concurrent.futures
from CONSTANT import *
from google.cloud import *
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = Credentials.from_service_account_file('google.json', scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# # open a google sheet
# gs = gc.open_by_key('1yO3H5mxusA5Ij_jKVkQYrlStTZbV7j2Zym75LueIhVM')
# # select a work sheet from its name
# worksheet1 = gs.worksheet('Sheet1')

# Global variable to store cached responses
cached_responses = {}

async def fetch_data(session, country, code, table_index):
    # Check if the response is already cached
    if (country, code) in cached_responses:
        html = cached_responses[(country, code)]
    else:
        url = f"https://core.aiesec.org.eg/analytics/{code}/LC24/"
        async with session.get(url) as response:
            html = await response.text()
            # Cache the response
            cached_responses[(country, code)] = html
    tables = pd.read_html(html, flavor='lxml')
    df = tables[table_index] if isinstance(table_index, int) else tables[table_index]
    df["Country"] = country

    ### DATA CLEANING ###
    pattern = r'^[\W_]+$'  # Matches any non-word character or underscore
    # Apply the pattern to the ('Entity', 'Entity') column and filter rows
    df = df[~df[('Entity', 'Entity')].astype(str).str.match(pattern)]

    # Drop rows containing 'closed' in ('Entity', 'Entity') column
    df = df[~df[('Entity', 'Entity')].str.lower().str.contains('closed')]

    # Mask and Apply the mask to filter out rows which contain 'NOT' in ('Entity', 'Entity') column
    mask = df[('Entity', 'Entity')].str.strip().str.upper() != 'NOT'
    df = df[mask]

    print(f"-- Processed {table_index}'th table of {country} --")
    return df

async def scrape_from_core_async(session, country_codes_urls, table_index):
    tasks = [fetch_data(session, country, code, table_index) for country, code in country_codes_urls.items()]
    dfs = await asyncio.gather(*tasks)
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df

async def main():
    table_indices = [0, 1, 2, 3, 4, 5]
    # table_indices = [0, 1]
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=20)) as session:
        tasks = [scrape_from_core_async(session, country_codes_urls, table_index) for table_index in table_indices]
        abs_df, cr_applicant_df, cr_accepted_df, cr_approved_df, cr_realized_df, cr_finished_df = await asyncio.gather(*tasks)
        # abs_df, cr_applicant_df = await asyncio.gather(*tasks)
    
        # abs_df = abs_df.transpose()
        # cr_applicant_df = cr_applicant_df.transpose()
    return abs_df, cr_applicant_df, cr_accepted_df, cr_approved_df, cr_realized_df, cr_finished_df
    # return abs_df, cr_applicant_df


async def run_main():
    return await main()

async def process():

    abs_df, cr_applicant_df, cr_accepted_df, cr_approved_df, cr_realized_df, cr_finished_df = await run_main()
    
    # Define spreadsheet IDs
    spreadsheet_ids = ['1yO3H5mxusA5Ij_jKVkQYrlStTZbV7j2Zym75LueIhVM',
                        '1gxrKQjHaCDXl2KiNvZSznKcYYR6QCtGK_Hsfx4VbZ2A',
                        '1H_GM7HB_aG4zw8SaY_OAwaWF_szsNZzRIMk4qn_SH7E',
                        '1wRdEZkAUVFJAkqDF2_dJeEWctOpnBAuT78J04U',
                        '17cJ67i2Lh30jNHiL4QVJmpQ2NygZN2pXJGeACJLsAOw',
                        '14agCxr-g6GdRduw4o-S8fBpKieevD-ZXGVdgDbTFwRA',
                        ]
    
    # Define dataframe list
    df_list = [abs_df, cr_applicant_df, cr_accepted_df, cr_approved_df, cr_realized_df, cr_finished_df]

    # Create tasks to concurrently write dataframes to spreadsheets
    async def write_to_spreadsheet(spreadsheet_id, df):
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.sheet1  # You might want to adjust this depending on your sheet structure
        worksheet.clear()
        set_with_dataframe(worksheet=worksheet, dataframe=df, include_index=False, include_column_header=True, resize=True)

    write_tasks = [write_to_spreadsheet(spreadsheet_id, df) for spreadsheet_id, df in zip(spreadsheet_ids, df_list)]
    
    # Run tasks concurrently
    await asyncio.gather(*write_tasks)
    
    
    # write to dataframe

    # abs_df, cr_applicant_df = await run_main()
    # # abs_df.to_csv("abs_df.csv", index=False)
    # # print(cr_applicant_df.columns)
    # # print(cr_applicant_df)
    # # cr_applicant_df.to_csv("cr_applicant_df.csv", index=False)

    # worksheet1.clear()
    # set_with_dataframe(worksheet=worksheet1, dataframe=abs_df, include_index=False,
    # include_column_header=True, resize=True)


    # abs_df, cr_applicant_df, cr_accepted_df, cr_approved_df, cr_realized_df, cr_finished_df = await run_main()
    # abs_df, cr_applicant_df = await run_main()
    # print(abs_df.columns)
    # print(abs_df)
    # abs_df.to_csv("abs_df_t.csv", index=False)
    # print("-- CR APPLICANT --")
    # print(cr_applicant_df.columns)
    # print(cr_applicant_df)
    # cr_applicant_df.to_csv("cr_applicant_df_t.csv", index=False)
    # print("-- CR ACCEPTED --")
    # print(cr_accepted_df.columns)
    # print(cr_accepted_df)
    # cr_accepted_df.to_csv("cr_accepted_df.csv", index=False)
    # print("-- CR APPROVED --")
    # print(cr_approved_df.columns)
    # print(cr_approved_df)
    # cr_approved_df.to_csv("cr_approved_df.csv", index=False)
    # print("-- CR REALIZED --")
    # print(cr_realized_df.columns)
    # print(cr_realized_df)
    # cr_realized_df.to_csv("cr_realized_df.csv", index=False)
    # print("-- CR FINISHED --")
    # print(cr_finished_df.columns)
    # print(cr_finished_df)
    # cr_finished_df.to_csv("cr_finished_df.csv", index=False)

asyncio.run(process())
