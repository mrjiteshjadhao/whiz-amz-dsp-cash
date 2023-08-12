from whizzbox import amazon_supplementary_reports as asr
from whizzbox import toolkit, custom_errors, config, s3_connector as s3c
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import time
from whizzbox import db_connector, amazon_sites

pd.options.mode.chained_assignment = None   # removes settingwithcopy warning

PROJECT_NAME = 'whiz-amz-dsp-cash'
HEADLESS = True
TEST = True  # if it's a test, email recipients are limited
SAMPLE = False  # if it's a sample, no. of sites are limited
SEND_EMAIL = False
SEND_FAIL_EMAIL = True


def download_zipfile_and_convert_to_df(amz_username, amz_password, zip_folder,
                                       zip_filename, csv_folder, cookie_path, stn_code, headless=True):
    """Downloads the Zip file from amazon logistics page, extracts and convert it into Dataframe"""
    browser = toolkit.get_driver(downloads_folder=zip_folder, headless=headless)
    amz_logistics_homepage = 'https://logistics.amazon.in/account-management/dashboard'
    logged_in = toolkit.check_login_success(toolkit.login_to_amazon(driver=browser,
                                                                    url=amz_logistics_homepage,
                                                                    username=amz_username,
                                                                    password=amz_password,
                                                                    cookie_path=cookie_path),
                                            expected_url=amz_logistics_homepage)
    if logged_in:

        edsp_csv_path = asr.extract_the_zipfile(zip_file_path=asr.download_the_zipfile(driver=browser,
                                                                                       zip_name=zip_filename,
                                                                                       downloads_folder=zip_folder,
                                                                                       stn_code=stn_code),
                                                protected=True,
                                                zip_pwd='amazon123*',
                                                destination_folder=csv_folder)
        if '.csv' in edsp_csv_path:
            df = pd.read_csv(edsp_csv_path)
        else:
            df = pd.read_excel(edsp_csv_path, sheet_name=1)
    else:
        raise custom_errors.LoginError
    browser.close()
    browser.quit()
    return df


def format_date_column(df, date_column_name):
    df[f'{date_column_name}_new'] = df[date_column_name].str.split(' ', expand=True)[0]
    return df


def get_current_month_driver_level(df):
    final_df = df.copy()
    current_date = datetime.now(config.tz)
    current_month_str = current_date.strftime("%Y-%m-")
    final_df = final_df[final_df['debrief_date'].astype('str').str.contains(current_month_str)]
    final_df = final_df.groupby(['employee_name', 'station'])['submitted_short_excess'].sum().reset_index()
    final_df.insert(loc=0, column='date', value=current_date.strftime("%Y-%m-%d"))
    return final_df


if __name__ == '__main__':
    load_dotenv()
    tz = config.tz

    t1 = time.time()  # execution start time for the python script
    print('\n--------------------***--------------------\n')
    print(f'Execution Started at: {datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Test:{TEST} | Sample:{SAMPLE} | Send Email:{SEND_EMAIL} | Headless:{HEADLESS}')

    data_folderpath = toolkit.create_folder(projectname=PROJECT_NAME, foldername='data')
    temp_zip_folderpath = toolkit.create_folder(projectname=PROJECT_NAME, foldername='temp_zip')
    extracted_csv_folderpath = toolkit.create_folder(projectname=PROJECT_NAME, foldername='temp_ext_csv')
    final_output_fname = f'{datetime.now(tz).strftime("%Y-%m-%d")}_AmazonDSP_Report.xlsx'
    final_output_fpath = data_folderpath + '/' + final_output_fname

    amazon_logistics_cookie = f'/home/ubuntu/atom/{PROJECT_NAME}/cookies.json' if config.ON_SERVER \
        else f'/Users/Admin/PycharmProjects/{PROJECT_NAME}/cookies.json'

    toolkit.clean_temp_folders(folders_list=[temp_zip_folderpath, extracted_csv_folderpath])

    try:
        dsp_cash_df_raw = download_zipfile_and_convert_to_df(amz_username=os.getenv('AMZ_ID'),
                                                             amz_password=os.getenv('AMZ_PASSWORD'),
                                                             zip_folder=temp_zip_folderpath,
                                                             zip_filename=' DSP Short Cash to be Submitted.zip',
                                                             csv_folder=extracted_csv_folderpath,
                                                             cookie_path=amazon_logistics_cookie,
                                                             headless=HEADLESS,
                                                             stn_code='BLT2')

        dsp_loss_df_raw = download_zipfile_and_convert_to_df(amz_username=os.getenv('AMZ_ID'),
                                                             amz_password=os.getenv('AMZ_PASSWORD'),
                                                             zip_folder=temp_zip_folderpath,
                                                             zip_filename='Potential losses_ageing report_ZIPZ.zip',
                                                             csv_folder=extracted_csv_folderpath,
                                                             cookie_path=amazon_logistics_cookie,
                                                             headless=HEADLESS,
                                                             stn_code='TRLD')

        dsp_oor_df_raw = download_zipfile_and_convert_to_df(amz_username=os.getenv('AMZ_ID'),
                                                            amz_password=os.getenv('AMZ_PASSWORD'),
                                                            zip_folder=temp_zip_folderpath,
                                                            zip_filename='OOR_Pickup done Packages with '
                                                                         'no RTS scan_ZIPZ.zip',
                                                            csv_folder=extracted_csv_folderpath,
                                                            cookie_path=amazon_logistics_cookie,
                                                            headless=HEADLESS,
                                                            stn_code='HYDH')

        dsp_cash_df = get_current_month_driver_level(df=dsp_cash_df_raw)

        amazon_sites_df = amazon_sites.create_amazon_sites_df(db=db_connector.connect_to_db(db_name='whizzard'))
        amazon_sites_df = amazon_sites_df[['Client Site Code', 'OM Name', 'RM Name', 'Client']]
        amazon_sites_df.columns = ['station', 'om_name', 'rm_name', 'client_name']

        dsp_cash_df_final = pd.merge(dsp_cash_df, amazon_sites_df, on='station', how='left')
        dsp_loss_df_raw = pd.merge(dsp_loss_df_raw, amazon_sites_df, on='station', how='left')
        dsp_oor_df_raw = dsp_oor_df_raw.rename(columns={'delivery_station_code': 'station'})
        dsp_oor_df_raw = pd.merge(dsp_oor_df_raw, amazon_sites_df, on='station', how='left')

        target_col = dsp_cash_df_final.pop('submitted_short_excess')
        dsp_cash_df_final.insert(loc=dsp_cash_df_final.shape[1], column='submitted_short_excess', value=target_col)
        dsp_loss_df_raw = format_date_column(df=dsp_loss_df_raw, date_column_name='event_datetime')
        dsp_loss_df_raw = format_date_column(df=dsp_loss_df_raw, date_column_name='at_station_time')
        dsp_loss_df_raw = dsp_loss_df_raw.drop_duplicates(subset=['tracking_id'])
        dsp_loss_df_raw = dsp_loss_df_raw.reset_index(drop=True)

        message = [f"""Hi Team,\nPlease find the attached associate level report for the Amazon DSP sites.\
        \n\nRegards\nAnalytics Team"""]

        from_address = os.getenv('EMAIL_ID')

        if TEST:
            to_address = ['mrjiteshjadhao@gmail.com']
            cc_address = ['mrjiteshjadhao@gmail.com']
        else:
            to_address = ['jitesharvindjadhao1999@gmail.com']
            cc_address = ['mrjiteshjadhao@gmail.com']
        subj = f'{final_output_fname.replace(".xlsx", "")}'

        with pd.ExcelWriter(final_output_fpath, engine=None) as writer:
            dsp_cash_df_final.to_excel(writer, sheet_name='DSP Drivers Cash', index=False)
            dsp_loss_df_raw.to_excel(writer, sheet_name='DSP Potential Loss', index=False)
            dsp_oor_df_raw.to_excel(writer, sheet_name='DSP OOR No RTS', index=False)

        toolkit.send_email(send=SEND_EMAIL, from_email=from_address,
                           pwd=os.getenv('EMAIL_PASSWORD'),
                           receiver_email=to_address, copy=cc_address, email_subject=subj,
                           email_message=message, attachment_file=[final_output_fpath])

        s3_storage = s3c.connect_to_s3_storage(os.getenv('AWS_ACCESS_KEY_ID'),
                                               os.getenv('AWS_SECRET_ACCESS_KEY'))
        atom_bucket = s3_storage.Bucket('atom-s3')  # selecting a bucket from the s3 storage

        if not SAMPLE:  # upload final output into s3 bucket, only if it's not sample
            s3c.upload_to_s3(atom_bucket, 'whiz-amz-dsp-drivers-cash', final_output_fpath, final_output_fname)

    except Exception as e:
        message = f'Error occured while generating the report!\nError:{type(e).__name__, e}'
        print(message)
        subj = f'Failed: {final_output_fname.replace(".xlsx", "")}'
        toolkit.send_failure_email(send=SEND_FAIL_EMAIL, from_email=os.getenv('EMAIL_ID'),
                                   pwd=os.getenv('EMAIL_PASSWORD'),
                                   receiver_email='mrjiteshjadhao@gmail.com',
                                   email_subject=subj, email_message=message)
