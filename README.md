# Introduction
* The scripts in this branch implement a stop-gap solution for the ASR workflow using OLIVE. 
Its purpose is to enable teams to experiment with the ASR service while a more robust API solution 
can be developed (expected by end of August)
* The current version of the demo only supports transcribing audio files in Mandarin

# Requirements and Setup
* After cloning the branch, ensure that you are in the **olive-demo-v1** branch
* Install the dependency packages by executing the following command
  
  ```
  pip install -r requirements.txt
  ```

# Executing ASR Workflows

## General Description
This section contains a general overview of the ASR workflow
* The OLIVE platform was packaged and shipped in the form of a Docker image. We have deployed this image on an EC2 instance in AWS
* To process files through OLIVE, the input files must be present locally on the machine running the Docker container
* To enable this, we provide a utility script to upload files from your machine to an S3 bucket in our AWS environment, from where we download the files to the EC2 instance on which the OLIVE Docker container is deployed. 
* Once processing through OLIVE is complete, the results are parsed and saved back in an S3 bucket in our AWS environment
* These results can then be fetched using the same utility script used for uploading files. 

## Uploading Files
* To upload files from your local run. 

    ```
    python3 utils.py --mode=upload \
                     --input_files=./path/to/data/dir/or/single/file \
                     --config_file=./aws_config.yml
    ```
    **Note:** Ensure you have the correct parameter settings in the aws_config.yml file

* Once the above script has finished executing, you should be able to see a new directory called **results**
  * Within that you should see a subdirectory of the form YYYYMMDD-hhmmss (Y=Year, M=Month, D=Day, h=Hour, m=Minute, s=Second). 
  * Within this subdirectory there should be a .txt file of the form filelist-YYYYMMDD-hhmmss.txt. This .txt file contains a list of files that have been successfully uploaded to the S3 bucket in AWS and for which the OLIVE workflow will be executed. 
* 3 sample files have been provided in the **data** directory. To execute the demo on these files run
    ```
    python3 utils.py --mode=upload \
                     --input_files=./data \
                     --config_file=./aws_config.yml
    ```


## Fetching ASR Results
* To fetch the JSON response from the ASR workflow run
    
    ```
    python3 utils.py --mode=fetch \
                     --filelist=/path/to/filelist \
                     --config_file=./aws_config.yml
    ```
    
    **Note:** The filelist was previously created during the upload step

* The output from the above command is saved in the **./results** directory under the same subdirectory as the filelist created during the upload step
* A sample filelist has been provided in the **results** directory. To execute the demo on this file run
    ```
    python3 utils.py --mode=fetch \
                     --filelist=./results/20220806-230018/filelist-20220806-230018.txt \
                     --config_file=./aws_config.yml
    ```
