
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
import os

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
creds = {
  "type": "service_account",
  "project_id": "aiesec-data",
  "private_key_id": "9edf63e4dd234b1081472258729e3ce01f0ada05",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDAjbMwlQ7hxy2i\nR/Pa/WOPFc47uYNkvlpcboKv7FHsmdazjQyGTMZsbT+NlrKQN5RwPx0AO5WuuXkl\n/VxNPtc27yXbDOmKKGL52LI2TxexrDBQWo+T8VS0PCHFj01oixo0gmlLV1MucKBH\nhpnbeYlRwyceoXK91miFd/4JWlilTMo02HS/1dnzoILxGhDEw3c/BQ/Bq9B63Gx2\nEqbzlctPvZ55apOsoyvFa6ef4HQXPEoxoWniXAW8zNuW09b6LcRqtLNhnI1Y3MEY\nxChd8up2W/lJy9O4VE83WhVKY44LwEjCyS1/WVsSD9Ovpvb/mU3mTHMPGWz+w0cY\nt4oDSKRBAgMBAAECggEABPhnZ6WyWhU/zh6CurxzkmMFBT1GOS6Rcy06V/+O9A86\nx7zYZdtTnCMECuPo4TCBFzZm2Y2gMQ/aMucHx9nrWOB6kv1h5iJBn0rG22BmZ3x/\nDQTwa7iFF+etNL38qNod250B6isjc1l+hqggnrXj+7mAkBGpbkFrhmX095F2pDIk\nJKbwprkV3coUNiL8VdjZqOL3qigzEh/iBCWSAV2zak2p1reeMQZ594MLhHQiNQBa\nq2I9mscd7IJI1qrhFnO2bEB+cSJHtZJXHme5krXxW2MVRK60NYrfyG1JMxOI7s3W\npxqmawkmZQ576BlwwsTeICT8HwcBesZWfGNjyx1FEQKBgQD5CC1k8AcDoc95ck+1\nL01VNIPU9CUnD6V5p6+mEhqe1L+6ZTfpfLczNVSVT0io3uk84S0uIJ0MmiUszmkp\n4KW3ZzUCeNwchfxO4mPBNAqndMSsKs0Un91oYQICmP1xMbAIQhIQWXblJtYq4JV8\n9IotQeDRPM2oBgdpWHpv6R34kQKBgQDF8PdStkON7/28S3Nu82YSfnqGkjAnGFm9\n+o1QggmbTwwh0edfuLTcPOVKIt51bMTwPf8DOTbmDIn9HxNs26sfBIORYDEUvA53\n6CUHZGSphNu+igkekLXhcqyvJjoCNF/zowLx4LoOmymfys6Sr/zEPFPot4UhltyS\nBjV9OLdIsQKBgQCGyP1Ax4UUWyzfL5aimxKBDmZYCTheluJaIP85pEzSMJYA/a2w\navcA+VlpYrsR42xbrgh1EePOoVODl8hliRhIVszjJKUFryMrUu7o8BDgJ5wXDydm\nhUwhpXegDkwGSv1ayt/aB4IJbua12E4wjm6HJkPXG9C3o2O5idDi50p4cQKBgFNc\n/bx++IqBpXo0yKPUrItjaxTb4p3Eep8xM2zRp1waeYCQ14IP11Pd7a9AajJIIdUQ\nNyNTaFSQuVi6SsMQ7Mu+ae7C9UjHPsyvH1EyrdZbFvTzS43s4jzVv/ZwAgRFrJd+\nctINlC5smKYskr1ikvDOe4RKLi6MS87QAJ0BUuexAoGARFw4hqvxBx6HYYllVyg9\nI0nKaXPNMYaEEOTwdM1/NDp4xbb31taadq3B5g0hmmv0B6BXx9A4/hhCPN3i6Q3G\n+WseFIX9eq78t/sSJdb1iaFmxVqmDqdo8ynLhe/oMLJWe27E8nlJZOOF5/+06GkR\nnKiz2ljtCyNY70XG7SfH3jg=\n-----END PRIVATE KEY-----\n",
  "client_email": "lc-rank-dashboard@aiesec-data.iam.gserviceaccount.com",
  "client_id": "116706230928808272904",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/lc-rank-dashboard%40aiesec-data.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

credentials = Credentials.from_service_account_file(creds, scopes=scopes)

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
