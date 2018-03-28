import os
from pathlib import Path
import click
import subprocess
import yaml
import random
from tempfile import TemporaryFile
from pathlib import Path
import pandas as pd
from collections import OrderedDict
import random
from time import sleep
import asyncio
from pprint import pprint
import getpass
import re


class Qwatch(object):
    """
    A class for parsing "qstat -f" output on SGE systems for monitoring
    jobs and making smart decisions about resource allocation.
    """
    def __init__(self, jobs: list=None, metadata: list=None, email: str=None, infile: str=None, watch: (bool or None)=None, plot: (bool or None)=None,
                 filename_pattern: str=None, directory: str='.', users: list = list([os.getlogin()]), cmd: str="qstat -f"):
        self.cmd = cmd
        if not users:
            self.users = []
        else:
            self.users = users
        if not jobs:
            self.jobs = []
        else:
            self.jobs = jobs
        self.metadata = metadata
        self.email = email
        self.infile = infile
        self.watch = watch
        self.plot = plot
        self.directory = directory
        self.filename_pattern = filename_pattern
        self.qstat_filename = Path()
        self.yaml_filename, self.metadata_filename, self.vl_metadata_filename, self.plot_filename = Path(), Path(), Path(), Path()
        self._yaml_config = "qwatch/qstat_dict.yml"
        self.initialize_data_files()
        self._setup_yaml()

    @classmethod
    def represent_dictionary_order(self, cls, dict_data):
        return cls.represent_mapping('tag:yaml.org,2002:map', dict_data.items())

    def _setup_yaml(self):
        """ https://stackoverflow.com/a/8661021 """
        yaml.add_representer(OrderedDict, self.represent_dictionary_order)

    def full_workflow(self, parse, process, data, metadata, watch):
        if parse:
            self.parse_qstat_data()
            if metadata or data:
                if metadata:
                    _data = self.get_dataframes()
                if data:
                    _data = self.get_qstat_data(watch_flag=watch)
                return _data
            elif process:
                self.process_jobs(watch_flag=watch)

    def initialize_data_files(self):

        if not self.filename_pattern:
            if self.infile:
                filename_pattern = f"{self.infile}_qstat_data"
            elif len(self.users) == 1 and len(self.jobs) == 0:
                filename_pattern = f"{self.users[0]}_qstat_data"
            elif len(self.jobs) == 1 and len(self.users) == 0:
                filename_pattern = f"{self.jobs[0]}_qstat_data"
            else:
                current_user = os.getlogin()
                _id = random.randint(10000, 99999)
                filename_pattern = f"{current_user}_{_id}_qstat_data"
        self.filename_pattern = filename_pattern

        # Create file names using the pattern
        Path(self.directory).mkdir(parents=True, exist_ok=True)
        self.yaml_filename = Path(self.directory) / Path(f"{filename_pattern}.yml")
        self.metadata_filename = Path(self.directory) / Path(f"{filename_pattern}_metadata.csv")
        self.vl_metadata_filename = Path(self.directory) / Path(f"{filename_pattern}_vl_metadata.csv")
        self.plot_filename = Path(self.directory) / Path(f"{filename_pattern}.png")

    def parse_qstat_data(self):
        if self.infile:
            self.qstat_filename = Path(self.infile)
        else:
            self.qstat_filename = Path(self.directory) / Path(f"{self.filename_pattern}.txt")
            qstat = subprocess.Popen(self.cmd, stderr=subprocess.PIPE,
                                     stdout=subprocess.PIPE, shell=True,
                                     encoding='utf-8', universal_newlines=False)
            out = qstat.stdout.readlines()
            error = qstat.stderr.readlines()
            with open(self.qstat_filename, 'w') as qf:
                qf.writelines(out)
            print(error)

    def process_jobs(self, watch_flag=False):
        # Parse the qstat file and create a dictionary object
        job_dict = self.qstat_parser()
        print(job_dict)
        # Filter and keep only the selected jobs and then create a YAML file
        kept_jobs, kept_dict = self.filter_jobs(job_dict)
        print(f'jobs: {kept_jobs}')
        print(f'dict: {kept_dict}')
        if not self.yaml_filename.is_file() or watch_flag is True:
            self.jobs = kept_jobs
            with open(self.yaml_filename, 'w') as yf:
                yaml.dump(kept_dict, stream=yf, default_flow_style=False)
        else:
            with open(self.yaml_filename, 'r') as yf:
                test = yaml.load(yf)
                if not test:
                    with open(self.yaml_filename, 'w') as yf:
                        yaml.dump(kept_dict, stream=yf, default_flow_style=False)

    def qstat_parser(self):
        """
        The qstat parser takes the qstat file from the user infile or from the
        'qstat -f' command and parses it.  It uses the qstat keywords found in the
        qstat yaml file.
        :return:  A dictionary of jobs.
        :rtype:
        """
        mast_dict = OrderedDict()
        job_count = 0
        phrase_continuation_flag = None
        with open(self.qstat_filename, 'r') as qf:
            with open(self._yaml_config, 'r') as yf:
                qstat_keywords = yaml.load(yf)
                qstat_sentence = None
                continuation_phrase = ""
                qstat_phrase = ""
                prev_item = None
                for item in qf.readlines():
                    # If a new job is identified then create the nested dictionary
                    if "Job Id" in item:
                        job_count += 1
                        _ = item.split(": ")
                        job_id_key = "%s" % _[1].replace("\r\n", "")
                        job_id_key = job_id_key.replace("\n", "")
                        mast_dict[job_id_key] = OrderedDict()
                        mast_dict[job_id_key]["Job_Id"] = job_id_key
                    # The current line information is used to determine single or multi-lined parsing for the
                    # previous line.
                    # If the a new keyword is recognized, then parse the line.
                    elif "    " in item and any(kw in item for kw in list(qstat_keywords["Job Id"].keys()) + list(qstat_keywords["Job Id"]["Variable_List"].keys())) or item == "\n":
                        item = item.replace("    ", "")
                        # Join the multi-lined "phrases" into one "sentence"
                        if phrase_continuation_flag is True:
                            qstat_sentence = qstat_phrase + continuation_phrase
                            phrase_continuation_flag = False
                        #  If the phrase is a single line
                        elif qstat_phrase == prev_item:
                            qstat_sentence = qstat_phrase
                        # Updates the qstat phrase unless it's in between lines or the end of file
                        if item != "\n":
                            qstat_phrase = item
                        else:
                            print("New Job or end of file!")
                    # If there is no keyword and tabbed whitespace is recognized, then the current line
                    # is a continuation phrase for the most recent qstat keyword
                    elif "\t" in item:
                        phrase_continuation_flag = True
                        continuation_phrase = continuation_phrase + item

                    # For multi-line phrases format the qstat sentence
                    if phrase_continuation_flag is False:
                        qstat_sentence = qstat_sentence.replace("\n\t", "").replace('\n', '')
                        #print(f'sentence:  {qstat_sentence}')
                        continuation_phrase = ""
                        phrase_continuation_flag = None
                        qstat_list = qstat_sentence.split(" = ")
                        qstat_keyword = qstat_list[0]
                        qstat_value = qstat_list[1]
                        # The variable list is unique in that it can be split into a dictionary of
                        # environment variables
                        if qstat_keyword == "Variable_List":
                            qstat_value = qstat_value.split(",")
                            temp_dict = OrderedDict(var.split("=") for var in qstat_value)
                            for vl_key, vl_value in temp_dict.items():
                                if vl_value[0] == "/" or vl_value[0] == "\\":
                                    vl_value = Path(vl_value)
                                temp_dict[vl_key] = vl_value
                            mast_dict[job_id_key]["Variable_List"] = temp_dict
                        # All of the other qstat keywords/sentences are basic key/value pairs
                        else:
                            mast_dict[job_id_key][qstat_keyword] = qstat_value

                    # For single line Phrases
                    elif qstat_sentence:
                        qstat_sentence = qstat_sentence.replace("\n\t", "")
                        #print(f'sentence: {qstat_sentence}')
                        qstat_list = qstat_sentence.split(" = ")
                        qstat_keyword = qstat_list[0]
                        qstat_value = qstat_list[1]
                        mast_dict[job_id_key][qstat_keyword] = qstat_value.replace('\n', '')
                    qstat_sentence = None
                    prev_item = item
        return mast_dict

    def get_qstat_data(self, watch_flag=False):
        if not self.yaml_filename.is_file() or watch_flag is True:
            self.process_jobs(watch_flag=watch_flag)
            with open(self.yaml_filename, 'r') as yf:
                jobs_dict = yaml.load(yf)
        else:
            with open(self.yaml_filename, 'r') as yf:
                test = yaml.load(yf)
                if not test:
                    self.process_jobs()
                    with open(self.yaml_filename, 'w') as yf:
                        jobs_dict = yaml.load(yf)
        return jobs_dict

    def get_dataframes(self, watch_flag=False):
        jobs_dict = self.get_qstat_data(watch_flag=watch_flag)
        print(jobs_dict)
        df = OrderedDict()
        master_df = OrderedDict()
        vl_df = OrderedDict()
        md_df = OrderedDict()
        for job in jobs_dict.keys():
            row = OrderedDict()
            _job = OrderedDict()
            if not self.metadata:
                row = jobs_dict[job]
            else:
                for var in self.metadata:
                    item = jobs_dict[job][var]
                    row[var] = item
            _job[job] = row
            df.update(_job)
        # Rework this
        for job in df.keys():
            vl_df[job] = df[job]['Variable_List']
            md_df[job] = OrderedDict()
            for keyword in df[job].keys():
                if keyword != 'Variable_List':
                    md_df[job][keyword] = df[job][keyword]

        master_df["Variable_Lists"] = pd.DataFrame.from_dict(vl_df)
        master_df["Metadata"] = pd.DataFrame.from_dict(md_df)
        #pprint(vl_df)
        #pprint(md_df)
        #print(pd.DataFrame.from_dict(vl_df))
        #print(pd.DataFrame.from_dict(md_df))
        return master_df

    def get_metadata_df(self, watch_flag=False):
        master_df = self.get_dataframes(watch_flag=watch_flag)
        return master_df["Metadata"]

    def get_pbs_env_df(self, watch_flag=False):
        master_df = self.get_dataframes(watch_flag=watch_flag)
        return master_df["Variable_Lists"]

    def filter_jobs(self, job_dict):
        kept_jobs = []
        print(len(self.users))
        if len(self.jobs) != 0 or len(self.users) != 0:
            print('if')
            for j in job_dict.keys():
                if len(self.users) > 0:
                    if job_dict[j]["Job_Owner"] in self.users:
                        print(j)
                        kept_jobs.append(j)
                elif len(self.jobs) > 0:
                    if job_dict[j]["Job_Name"] in self.jobs:
                        kept_jobs.append(j)
                        print(j)
        else:
            print('else')
            kept_jobs += list(job_dict.keys())
        kept_jobs = list(set(kept_jobs))
        kept_dict = OrderedDict((k, job_dict[k]) for k in kept_jobs)
        return kept_jobs, kept_dict


class Qwaiter(Qwatch):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.qwatch = Qwatch

    def _get_subset_kwargs(self, skipped_kwargs):
        _kwargs = {}
        for var, attr in self.__dict__.items():
            if var not in skipped_kwargs:
                _kwargs[var] = attr
        return _kwargs

    def update_csv(self, file, data):
        if file.is_file():
            file_df = pd.read_csv(file)
            updated_df = pd.concat([file_df, data], axis=1)
            updated_df.to_csv(file)
        else:
            data.to_csv(file)

    def watch_jobs(self):
        ioloop = asyncio.get_event_loop()
        ioloop.run_until_complete(self._async_watch_jobs())
        ioloop.close()

    async def _async_watch_jobs(self):
        self.parse_qstat_data()
        self.process_jobs(watch_flag=True)
        tasks = [asyncio.ensure_future(self._async_watch(job)) for job in self.jobs]
        await asyncio.wait(tasks)

    async def _async_watch(self, job_id, sleeper=120):
        """Wait until a job or list of jobs finishes and get updates."""
        _kwargs = self._get_subset_kwargs(skipped_kwargs=["jobs", "directory"])
        watch_one = self.qwatch(jobs=[job_id], directory=f'{job_id}', **_kwargs)
        job_dict = watch_one.full_workflow(watch=True, parse=True, process=True, data=True, metadata=False)
        md = watch_one.get_metadata_df(watch_flag=True)
        ev = watch_one.get_pbs_env_df(watch_flag=True)
        self.update_csv(file=self.metadata_filename, data=md)
        self.update_csv(file=self.vl_metadata_filename, data=ev)

        if job_dict[job_id]['job_state'] == 'Q':
            yield 'Waiting for %s to start running.' % job_id
            sleep(sleeper)
            self.watch(job_id, sleeper=sleeper)
        elif job_dict[job_id]['job_state'] == 'R':
            yield 'Waiting for %s to finish running.' % job_id
            sleep(sleeper)
            self.watch(job_id, sleeper=sleeper)
