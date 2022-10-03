"""
Usage: python3 process_asr_olive.py --config_file=./aws_config.yml
"""

import os
import traceback
import argparse
import json
import yaml
import time
import pathlib
import subprocess

import pandas as pd
from IPython.display import display
import boto3

import utils


def read_pred_file(fname):
    try:
        with open(fname, 'r') as fh:
            data = fh.read()
            data = data.split("\n", 3)[-1]
            data = json.loads(data)[0]

    except Exception:
        print(f"ERROR IN READING {fname}")
        data = None

    return data


def get_input_fname(json_data):
    input_fname = json_data['data'][0]['data_id']

    return input_fname


def bsearch(intervals, speakers, entry, debug=False):
    left, right = 0, len(intervals) - 1

    while left <= right:
        mid = (left + right) // 2

        if debug:
            print(f"""mid={mid}, left={left}, right={right},
            entry['start_t']={entry['start_t']},
            intervals[m].left={intervals[mid].left},
            intervals[m].right={intervals[mid].right}\n""")

        if entry['start_t'] in intervals[mid]:
            return speakers[mid]

        elif entry['start_t'] > intervals[mid].right:
            left = mid + 1

        elif entry['start_t'] < intervals[mid].left:
            right = mid - 1

    return "No Speaker Found to Attribute!"


def assign_speaker_label(row, speaker_intervals):
    contributions = []
    for interval, speaker_label in speaker_intervals:
        if contributions and row['start_time'] > interval.right and row['end_time'] > interval.right:
            break

        if row['start_time'] in interval and row['end_time'] in interval:
            return speaker_label

        if row['start_time'] in interval and row['end_time'] not in interval:
            speaker_duration = interval.right - row['start_time']
            contributions.append([speaker_duration, speaker_label])

        elif row['start_time'] not in interval and row['end_time'] in interval:
            speaker_duration = row['end_time'] - interval.left
            contributions.append([speaker_duration, speaker_label])

        else:
            continue

    if contributions:
        speaker_label = max(contributions, key=lambda x: x[0])[1]
        return speaker_label
    else:
        return "No Speaker Found to Attribute!"


def get_asr_output(json_data):
    asr_result = json_data['tasks']['ASR'][0]['analysis']['region']

    text_data = [x['class_id'] for x in asr_result]
    text_out = " ".join(text_data)

    diarization_data = json_data['tasks']['SDD'][0]
    diarization_result = diarization_data['analysis']['region']

    speaker_intervals = [
                            [
                                pd.Interval(x['start_t'],
                                            x['end_t'],
                                            closed='both'),
                                x['class_id']
                            ] for x in diarization_result
                    ]

    consolidated_out = []
    for idx, entry in enumerate(asr_result):
        speaker_out = {
                        'start_time': entry['start_t'],
                        'end_time': entry['end_t'],
                        'transcript': entry['class_id'],
                        'speaker_id': None,
                        }

        consolidated_out.append(speaker_out)

    transcript_df = pd.DataFrame(consolidated_out)
    transcript_df['speaker_id'] = transcript_df.apply(lambda row: assign_speaker_label(row, speaker_intervals), axis=1)
    consolidated_out = transcript_df.to_dict(orient='records')

    return text_out, consolidated_out


def parse_json_out(fname):
    json_data = read_pred_file(fname)

    data_id = get_input_fname(json_data)

    transcript, consolidated_out = get_asr_output(json_data)

    response = {
                'data_id': data_id,
                'text': transcript,
                'asr_utterance_lvl': consolidated_out,
                }

    basename = os.path.basename(data_id).split(".")[0]

    output_filename = os.path.join(os.path.abspath('./results'),
                                   'processed',
                                   f"{basename}_processed_results.json")

    with open(output_filename, 'w') as fh:
        json.dump(response, fh, ensure_ascii=False)

        print(f"Successfully saved results for {basename} at {output_filename}!")

    return output_filename


def call_olive(filename):
    basename = os.path.basename(filename).split(".")[0]
    output_filename = os.path.join(os.path.abspath('./results'),
                                   'raw',
                                   f"{basename}_raw_results.json")

    cmd = f'''olivepyworkflow -i {filename} \
            /home/ubuntu/str8775/olive5.4.0/oliveAppData/workflows/mandarinASR.workflow.json > {output_filename}'''
    subprocess.call(cmd, shell=True)
    return output_filename


def process_main(args):
    """Defines Workflow for Uploading Files to AWS"""
    # --------- Create clients to connect to AWS -------- #

    # Fetch credentails from config file
    with open(args.config_file, 'r') as yml_file:
        config = yaml.safe_load(yml_file)

    bucket_name = config['bucket_name']
    s3_client = boto3.client('s3', **config['aws_credentials'])

    # --------- Read Status CSV from S3 Bucket ----------- #
    status_df = utils.read_status_file(config, s3_client)

    # --------- Fetch files to process ----------- #
    input_files = status_df.loc[status_df['olive_process_timestamp'].isnull(), "filename"].tolist()
    print(f"Found {len(input_files)} files to Process!\n")

    if input_files:
        pathlib.Path(input_files[0]).parent.mkdir(parents=True, exist_ok=True)

        try:
            for filepath in input_files:
                try:
                    utils.download_file(bucket_name, filepath, filepath, s3_client)

                    raw_results_fname = call_olive(os.path.abspath(filepath))

                    processed_results_fname = parse_json_out(raw_results_fname)

                    aws_basename = os.path.basename(processed_results_fname)
                    aws_object_name = os.path.join('results', aws_basename)
                    upload_status = utils.upload_file(processed_results_fname,
                                                    bucket_name,
                                                    s3_client,
                                                    aws_object_name)

                    if upload_status:
                        print(f"Successfully uploaded results for {aws_basename} to S3")

                        # update the status_csv with the process timestamp
                        key = os.path.join('data', os.path.basename(filepath))
                        status_df.loc[status_df['filename'] == key, "olive_process_timestamp"] = int(time.time())

                # if error occurs for any one file then continue
                except Exception:
                    print(traceback.format_exc())
                    continue

        except Exception:
            print(traceback.format_exc())
            pass

        # --------- Upload Updated Status CSV to S3 bucket ----------- #
        display(status_df)

        utils.update_status_df(config, status_df)

    return


def main(args):
    """Defines Main Execution"""

    process_main(args)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--config_file', type=str, default='./aws_config.yml',
                        help='''path to YAML config file
                                containing the aws credentials
                                and bucket name''')

    args = parser.parse_args()

    main(args)
