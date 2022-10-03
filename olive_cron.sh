#!/bin/bash
cd /home/ubuntu/str8775/ccu-scripts
CWD="$(pwd)"
echo $CWD

source /home/ubuntu/miniconda3/bin/activate
conda activate python3
chmod +x utils.py
chmod +x process_asr_olive.py 
python3 process_asr_olive.py --config_file=./aws_config.yml
