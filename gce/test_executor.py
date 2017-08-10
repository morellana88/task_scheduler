#!/usr/bin/env python

# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""

This sample script shows how to use the reusable Executor utility to
watch a topic and execute a command when a message is received

"""

import logging
import os
import sys

from cloud_handler import CloudLoggingHandler
from cron_executor import Executor
from os.path import expanduser

PROJECT = 'ti-ca-ml-start'  # change this to match your project

TOPIC1 = 'sleep_10'
TOPIC2 = 'sleep_20'

# get home user directory
home_dir = expanduser('~').replace('\\', '/')

script_path1 = os.path.abspath(os.path.join(os.getcwd(), 'task1.py'))
script_path2 = os.path.abspath(os.path.join(os.getcwd(), 'task2.py'))

#script_path = os.path.abspath(os.path.join(os.getcwd(), 'logger_sample_task.py'))
#sample_task = "python -u %s" % script_path
task1 = "python -u %s" % script_path1
task2 = "python -u %s" % script_path2

root_logger = logging.getLogger('cron_executor')
root_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)

cloud_handler = CloudLoggingHandler(on_gce=True, logname="task_runner")
root_logger.addHandler(cloud_handler)

# create the executor that watches the topic, and will run the job task
# test_executor = Executor(topic=TOPIC, project=PROJECT, task_cmd=sample_task, subname='sample_task')
executor1 = Executor(topic=TOPIC1, project=PROJECT, task_cmd=task1, subname='task1')
executor2 = Executor(topic=TOPIC2, project=PROJECT, task_cmd=task2, subname='task2')

# add a cloud logging handler and stderr logging handler
job_cloud_handler = CloudLoggingHandler(on_gce=True, logname=executor1.subname)
executor1.job_log.addHandler(job_cloud_handler)
executor1.job_log.addHandler(ch)
executor1.job_log.setLevel(logging.DEBUG)


job_cloud_handler = CloudLoggingHandler(on_gce=True, logname=executor2.subname)
executor2.job_log.addHandler(job_cloud_handler)
executor2.job_log.addHandler(ch)
executor2.job_log.setLevel(logging.DEBUG)

# watches indefinitely
executor2.watch_topic()
