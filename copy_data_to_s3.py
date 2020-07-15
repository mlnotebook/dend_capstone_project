import os
import boto3
import configparser
from botocore.exceptions import ClientError

def upload_file_to_S3(filename, key, bucket_name):
	"""Uploads a file to an S3 bucket.
	
	Args:
		filename (str): the path to the file to be uploaded.
		key (str): the path to save the file in the bucket.
		bucket_name (str): the name of the S3 bucket.
	"""
    s3.Bucket(bucket_name).upload_file(filename, key)

def check_exists(s3_service, bucket_name, key):
	"""Checks if a file already exists in a S3 Bucket.
	
	Args:
		s3_service (boto resource object): the s3 access object.
		bucket_name (str): the name of the S3 bucket.
		key (str): the path to save the file in the bucket.
	Returns:
		True if file exists in S3 bucket, else False
	"""
	
    try:
        s3_service.Object(bucket_name, key).load()
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True


if __init__ == "__main__":
	# Parse the config file
	config_file = './i94.cfg'
	assert(os.path.exists(config_file)),
		f"Config file not found at {config_file}"
	config = configparser.ConfigParser()
	config.read(config_file)

	REDSHIFT_ARN = config['IAM_ROLE']['ARN']
	S3_BUCKET = config['S3']['BUCKET']

	# Create the S3 Object
	s3 = boto3.resource('s3')

	# Set the paths for the data in Udacity Workspace
	immigration_dir = os.path.abspath('../../data/18-83510-I94-Data-2016')
	sas_data_dir = os.path.abspath('./data/sas_data')
	
	# Gather data files
	source_immigration_data = [os.path.join(immigration_dir, f) for f in os.listdir(immigration_dir)]
	source_sas_data = [os.path.join(sas_data_dir, f) for f in os.listdir(sas_data_dir)]
	source_temperature_data = [os.path.abspath('../../data2/GlobalLandTemperaturesByCity.csv')]
	source_airport_data = [os.path.abspath('./data/airport-codes_csv.csv')]
	source_us_cities_demographics = [os.path.abspath('./data/us-cities-demographics.csv')]

	# Upload the data to S3
	upload_dict = {'immigration': source_immigration_data,
				'sas_data': source_sas_data,
				'temperature': source_temperature_data,
				'airport': source_airport_data,
				'demographics': source_us_cities_demographics}

	print(f'S3 Bucket: {S3_BUCKET}')
	for dataset_name in upload_dict.keys():
		print(f'Copying: {dataset_name}')
		filenames = upload_dict[dataset_name]
		for filename in filenames:
			s3_filepath = os.path.join(dataset_name, os.path.basename(filename))
			if not check_exists(s3, S3_BUCKET, s3_filepath):
				print(f'\tCopying {os.path.basename(filename)} ...', end='', flush=True)
				upload_file_to_S3(filename, s3_filepath, S3_BUCKET)
				print('done!')
			else:
				print(f'\tExists: {s3_filepath}')