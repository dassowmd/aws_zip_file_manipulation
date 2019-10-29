import boto3
import zipfile
import logging
import io
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

class s_three_bucket_handler:
    def __init__(self, bucket):
        self.client = boto3.client('s3')
        self.bucket = bucket

    def get_file_keys_in_bucket(self):
        country_files = self.client.list_objects_v2(Bucket=self.bucket)
        logger.debug(country_files)
        file_names = []
        for obj in country_files['Contents']:
            file_names.append(obj['Key'])
        logger.info(file_names)
        return file_names

    def get_file_from_s3(self, filename):
        return self.client.get_object(Bucket=self.bucket, Key=filename)

class base_zip_handler:
    def __init__(self, s_three_obj, desired_zip_file_keys=None, desired_file_names=None):
        self.s_three_obj = s_three_obj
        self.desired_zip_file_keys = desired_zip_file_keys
        self.desired_file_names = desired_file_names

    def list_files_in_zip(self, zip):
        return zipfile.ZipFile(io.BytesIO(zip)).namelist()

    def return_desired_files_from_zip(self, zip, file_names):
        if type(file_names) is not list:
            raise TypeError('file_names object should be a list, not %s' %(type(file_names)))
        files = []
        for file_name in file_names:
            temp_file = zipfile.ZipFile(io.BytesIO(zip)).open(file_name, mode='r')
            files.append(temp_file)
        return files

    def extract_files_iterate_desired_zip_file_keys(self):
        if self.desired_file_names is None:
            raise Exception("desired_file_names is not set")
        output_files = []
        for key in self.desired_zip_file_keys:
            zip_file = self.s_three_obj.get_file_from_s3(filename=key)['Body'].read()
            temp_file_names = self.extract_desired_file_names_from_zip(zip_file=zip_file)
            output_files.extend(self.return_desired_files_from_zip(zip=zip_file, file_names=temp_file_names))
        return output_files

    def extract_desired_file_names_from_zip(self, zip_file):
        file_list = self.list_files_in_zip(zip_file)
        logger.info('key list: %s' % file_list)
        temp_files = []
        for file in file_list:
            if file in self.desired_file_names:
                temp_files.append(file)
        return temp_files

    def clean_data(self, df):
        raise BaseException

    def generate_df_from_csv_list(self, file_list):
        df = None
        for file in file_list:
            temp_df = pd.read_csv(file)
            if df is None:
                df = temp_df
            else:
                df = df.append(temp_df)
        return df


class iso_country_handler(base_zip_handler):
    def clean_data(self, df):
        df['English short name lower case'] = df['English short name lower case'].str.lower().str.strip()
        return df


class country_profile_handler(base_zip_handler):
    def clean_data(self, df):
        df['country'] = df['country'].str.lower().str.strip()
        return df

if __name__=='__main__':

    s_three_obj = s_three_bucket_handler(bucket='sample-code-dassow')
    # s_three_obj.get_file_keys_in_bucket()

    iso_handler_obj = iso_country_handler(s_three_obj=s_three_obj, desired_zip_file_keys=['countries-iso-codes.zip'], desired_file_names=['wikipedia-iso-country-codes.csv'])
    iso_extracted_files = iso_handler_obj.extract_files_iterate_desired_zip_file_keys()
    iso_df = iso_handler_obj.generate_df_from_csv_list(file_list=iso_extracted_files)
    iso_df = iso_handler_obj.clean_data(iso_df)

    profile_handler_obj = country_profile_handler(s_three_obj=s_three_obj, desired_zip_file_keys=['undata-country-profiles.zip'], desired_file_names=['country_profile_variables.csv'])
    profile_extracted_files = profile_handler_obj.extract_files_iterate_desired_zip_file_keys()
    profile_df = profile_handler_obj.generate_df_from_csv_list(file_list=profile_extracted_files)
    profile_df = profile_handler_obj.clean_data(profile_df)

    # Merge data Frames
    final_df = pd.merge(iso_df, profile_df, left_on='English short name lower case', right_on='country', how='inner')
    final_df.to_csv('output.csv')

