"""
Example Usage:

For uploading files
python3 utils.py --mode=upload \
                        --input_files=./data \
                        --config_file=./aws_config.yml

For fetching results
python3 utils.py --mode=fetch \
                 --config_file=./aws_config.yml \
                 --filelist=./results/20220806-214520/filelist.txt
"""

import os
import traceback
import argparse
import yaml
import glob
import time

import pandas as pd
from IPython.display import display
from io import StringIO
from datetime import datetime as dt

import boto3
from botocore.exceptions import ClientError

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def read_status_file(config, s3_client):
    """Reads the status_csv of processed files present in AWS

    :param config:
    :param s3_client:
    :return: status_df
    """

    object_name = config['status_csv_filename']
    bucket_name = config['bucket_name']

    try:
        csv_object = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        csv_string = csv_object['Body'].read().decode('utf-8')

        status_df = pd.read_csv(StringIO(csv_string))
    except ClientError as err:
        if err.response['Error']['Code'] == "NoSuchKey":
            print("No status_csv present. Creating a new one!\n")
            status_df = pd.DataFrame(columns=['filename',
                                              'olive_process_timestamp'])
        else:
            print(traceback.format_exc())
            raise err

    return status_df


def update_status_df(config, status_df):
    """Updates the status_csv present in AWS S3

    :param config:
    :param s3_client:
    :param status_df:
    :return: status_df
    """

    bucket_name = config['bucket_name']
    object_name = config['status_csv_filename']

    try:
        csv_buffer = StringIO()
        status_df.to_csv(csv_buffer, index=False, header=True)

        s3_resource = boto3.resource('s3', **config['aws_credentials'])
        s3_resource.Object(bucket_name, object_name).put(Body=csv_buffer.getvalue())

        print(f"\nSaved status_csv at {object_name}!")

    except ClientError as err:
        if err.response['Error']['Code'] == "NoSuchKey":
            print("Incorrect Filename provided for status_csv. Value={object_name}\n")

        else:
            print(traceback.format_exc())
            raise err

    return status_df


def upload_file(file_name, bucket, s3_client, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :return: True if file was uploaded, else False
    """

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(traceback.format_exc())
        return False

    return True


def delete_file(bucket_name, filename, s3_client):
    s3_client.Object(bucket_name, filename).delete()


def download_file(bucket_name, aws_filename, local_filename, s3_client):
    s3_client.download_file(bucket_name, aws_filename, local_filename)


def upload_main(args):
    """Defines Workflow for Uploading Files to AWS"""

    # ------ Get List of Files to Process and Create Output Dir------ #
    input_files = args.input_files

    if os.path.isdir(input_files):
        input_files = glob.glob(os.path.join(input_files, "*.flac")) + \
                      glob.glob(os.path.join(input_files, "*.wav")) + \
                      glob.glob(os.path.join(input_files, "*.mp3"))
    else:
        input_files = [input_files]

    print(f"FOUND {len(input_files)} files to upload!\n")

    if input_files:
        # --------- Create clients to connect to AWS -------- #

        # Fetch credentails from config file
        with open(args.config_file, 'r') as yml_file:
            config = yaml.safe_load(yml_file)

        bucket_name = config['bucket_name']
        s3_client = boto3.client('s3', **config['aws_credentials'])

        # --------- Read Status CSV from S3 Bucket ----------- #
        status_df = read_status_file(config, s3_client)

        # --------- Upload input speech to an S3 bucket ----------- #
        uploaded_files = {"filename": [],
                        "olive_process_timestamp": []}
        try:
            for filepath in input_files:
                try:
                    print(f"\nProcessing filepath={filepath}")
                    object_name = os.path.join("data", os.path.basename(filepath))

                    # check if the file has already been uploaded to AWS
                    if not status_df['filename'].str.fullmatch(object_name).any():

                        upload_correct = upload_file(filepath,
                                                    bucket_name,
                                                    s3_client,
                                                    object_name)

                        if not upload_correct:
                            continue

                        else:
                            print(f"Uploaded {object_name} to {bucket_name} in S3\n")

                            uploaded_files['filename'].append(object_name)
                            uploaded_files['olive_process_timestamp'].append(None)
                    else:
                        print(f"{object_name} already exists in bucket")
                        continue

                # if error occurs for any one file then continue
                except Exception:
                    print(traceback.format_exc())
                    continue

        except Exception:
            print(traceback.format_exc())
            pass


        # --------- Upload Updated Status CSV to S3 bucket ----------- #
        new_uploads_df = pd.DataFrame(uploaded_files)
        status_df = pd.concat([status_df, new_uploads_df], axis=0).reset_index(drop=True)

        print(f"Successfuly Uploaded {len(new_uploads_df)} files to S3\n")
        display(status_df)

        update_status_df(config, status_df)

    return new_uploads_df, status_df


def fetch_main(args):
    with open(args.filelist, 'r') as fh:
        files_to_fetch = fh.readlines()
        files_to_fetch = [x.strip('\n').strip() for x in files_to_fetch]

    if files_to_fetch:
        print(f"\nFound {len(files_to_fetch)} files to fetch results for")
        # --------- Create clients to connect to AWS -------- #

        # Fetch credentails from config file
        with open(args.config_file, 'r') as yml_file:
            config = yaml.safe_load(yml_file)

        bucket_name = config['bucket_name']
        s3_client = boto3.client('s3', **config['aws_credentials'])

        # --------- Read Status CSV from S3 Bucket ----------- #
        status_df = read_status_file(config, s3_client)

        files_to_fetch_df = status_df.loc[status_df["filename"].isin(files_to_fetch)]

        # set the initial download status as False for all files
        files_to_fetch_df.loc[:, 'download_status'] = False

        results_dir = os.path.dirname(args.filelist)

        print(f"Saving results in {results_dir}")

        downloaded_files = []
        # iterate while there are files which have not been downloaded
        while not files_to_fetch_df.loc[~files_to_fetch_df['download_status']].empty:
            # get the list of files that have been processed through OLIVE
            completed_df = files_to_fetch_df.loc[~files_to_fetch_df["olive_process_timestamp"].isnull()]

            for filename in completed_df['filename'].tolist():
                basename = os.path.basename(filename).split(".")[0]
                mod_basename = f"{basename}_processed_results.json"

                aws_filename = os.path.join("results", mod_basename)
                local_filename = os.path.join(results_dir, mod_basename)

                # download the processed JSON results
                download_file(bucket_name, aws_filename, local_filename, s3_client)
                print(f"\nSuccessfully downloaded results for {basename} to {local_filename}")

                downloaded_files.append(filename)

            if len(completed_df) != len(files_to_fetch_df):
                count = len(files_to_fetch_df) - len(completed_df)
                print(f"{count} files are still being processed!")

                time.sleep(5)

                # get the latest status df
                status_df = read_status_file(config, s3_client)

                files_to_fetch_df = status_df.loc[status_df["filename"].isin(files_to_fetch)]
                files_to_fetch_df.loc[:, 'download_status'] = False
                files_to_fetch_df.loc[files_to_fetch_df['filename'].isin(downloaded_files), 'download_status'] = True

            else:
                break

    return


def main(args):
    """Defines Main Execution"""

    print(f"RUNNING SCRIPT IN {args.mode} MODE!")

    results_subdir = dt.now().strftime("%Y%m%d-%H%M%S")
    results_dir = os.path.join("./results", results_subdir)

    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    if args.mode == "upload":
        new_uploads_df, status_df = upload_main(args)

        new_filenames = new_uploads_df['filename'].tolist()

        filelist = os.path.join(results_dir, f"filelist-{results_subdir}.txt")
        with open(filelist, 'w') as fh:
            fh.write("\n".join(new_filenames))

    else:
        assert args.filelist is not None, "Must provide a list of files for which you want to fetch results"
        fetch_main(args)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--mode', choices=['upload', 'fetch'],
                        type=str, required=True, default='upload',
                        help='''specify the mode in which the
                                script must be executed''')
    parser.add_argument('--input_files', type=str, default=None,
                        help='''specify the path to single file to be
                                processed or dir containing
                                multiple files to be processed''')
    parser.add_argument('--config_file', type=str, default='./aws_config.yml',
                        help='''path to YAML config file
                                containing the aws credentials
                                and bucket name''')
    parser.add_argument('--filelist', type=str, default=None,
                        help='''.txt file containing list of files for which
                                you want to fetch asr results''')

    args = parser.parse_args()

    main(args)
