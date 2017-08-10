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
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

PROJECT = 'ti-ca-ml-start'  # change this to match your project
cron_entries = [('sleep_10', 'gce/task1.py'), ('sleep_20', 'gce/task2.py')]

# get home user directory
home_dir = expanduser('~').replace('\\', '/')

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


def create_tasks((topic_name, script_path)):
    abs_path = os.path.abspath(os.path.join(os.getcwd(), script_path))
    task = "python -u %s" % abs_path
    executor = [Executor(topic_name, project=PROJECT, task_cmd=task, subname='test')]
    job_cloud_handler = CloudLoggingHandler(on_gce=True, logname=executor.subname)
    executor.job_log.addHandler(job_cloud_handler)
    executor.job_log.addHandler(ch)
    executor.job_log.setLevel(logging.DEBUG)
    executor.watch_topic()

pool = ThreadPool(4)
results = pool.map(create_tasks, cron_entries)
pool.close()
pool.join()


