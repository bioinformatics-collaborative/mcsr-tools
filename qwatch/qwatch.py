import os
import click
import subprocess
import yaml
from pathlib import Path
import pandas as pd
from collections import OrderedDict
from dateutil import parser
import random
from datetime import datetime
from time import sleep
from pprint import pprint


class Qwatch(object):
    # Static qstat Keywords
    __misc_kw = ["Checkpoint", "Error_Path", "exec_host", "exec_vnode", "Hold_Types", "Join_Path",
                             "Keep_Files", "Mail_Points", "Output_Path", "Rerunable", "Resource_List.mpiprocs",
                             "Resource_List.ncpus", "Resource_List.nodect", "Resource_List.nodes",
                             "Resource_List.place", "Resource_List.select", "jobdir", "Variable_List", "umask",
                             "project", "Submit_arguments"]
    __job_limits_kw = ["ctime", "etime", "qtime", "stime", "mtime", "Resource_List.walltime", "Resource_List.cput",
                           "Resource_List.mem"]
    __job_time_kw = ["ctime", "etime", "qtime", "stime", "mtime"]
    __job_info_kw = ["Job_Id", "Job_Name", "Job_Owner", "queue", "server", "session_id"]
    __static_kw = __job_info_kw + __job_limits_kw + __misc_kw
    # Dynamic qstat Keywords
    __misc_data_kw = ["job_state", "Priority", "substate", "comment", "run_count"]
    __job_data_kw = ["resources_used.cpupercent", "resources_used.cput", "resources_used.mem",
                            "resources_used.vmem", "resources_used.walltime", "resources_used.ncpus"]
    __dynamic_kw = __job_data_kw + __misc_data_kw
    # All Keywords
    __keywords = __static_kw + __dynamic_kw
    # Metadata options
    __metadata_dict = {"environment variables": "Variable_List",
                       "plot": {"limits": __job_limits_kw,
                                "info": __job_info_kw,
                                "data": __job_data_kw},
                       "all": __keywords
                       }
    """
    A class for parsing "qstat -f" output on SGE systems for monitoring
    jobs and making smart decisions about resource allocation.
    """
    def __init__(self, jobs: (list or str)=None, metadata: list=None, email: str=None, infile: str=None, watch: (bool or None)=None, plot: (bool or None)=None,
                 filename_pattern: str=None, directory: str='.', users: (list or str) = os.getlogin(), cmd: str="qstat -f", sleeper: int=120):
        self.cmd = cmd
        # Get a user list
        if not users:
            self.users = []
        elif isinstance(users, str):
            self.users = [users]
        elif isinstance(users, list):
            self.users = users
        else:
            raise TypeError("The users parameter is a single user string or a multi-user list.")
        # Get a job list
        if not jobs:
            self.jobs = []
        elif isinstance(jobs, str):
            self.jobs = [jobs]
        elif isinstance(jobs, list):
            self.jobs = jobs
        else:
            raise TypeError("The jobs parameter is a single job string or a multi-job list.")

        self.metadata = metadata
        self.email = email
        self.infile = infile
        self.watch = watch
        self.plot = plot
        self.directory = directory
        self.filename_pattern = filename_pattern
        self.sleeper = sleeper
        self.qstat_filename = Path()
        self.yaml_filename = Path()
        self.data_filename = Path()
        self.info_filename = Path()
        self.plot_filename = Path()
        self._yaml_config = "qstat_dict.yml"

        # Other initial configuration
        self.initialize_data_files()
        self._setup_yaml()

    @classmethod
    def represent_dictionary_order(self, cls, dict_data):
        return cls.represent_mapping('tag:yaml.org,2002:map', dict_data.items())

    def _setup_yaml(self):
        """ https://stackoverflow.com/a/8661021 """
        yaml.add_representer(OrderedDict, self.represent_dictionary_order)

    def full_workflow(self, parse, process, data, metadata):
        if parse:
            self.parse_qstat_data()
            if metadata or data:
                if metadata:
                    _data = self.get_dicts()
                if data:
                    _data = self.get_qstat_data()
                return _data
            elif process:
                self.process_jobs()

    def initialize_data_files(self):
        # Get a filename pattern based on other user input
        if not self.filename_pattern:
            print("not_fnp")
            if self.infile:
                filename_pattern = f"{self.infile}"
            elif len(self.users) == 1 and len(self.jobs) == 0:
                filename_pattern = f"{self.users[0]}"
            elif len(self.jobs) == 1 and len(self.users) == 0:
                filename_pattern = f"{self.jobs[0]}"
            else:
                current_user = os.getlogin()
                _id = random.randint(10000, 99999)
                filename_pattern = f"{current_user}_{_id}"
            self.filename_pattern = filename_pattern

        # Create file names using the pattern
        Path(self.directory).mkdir(parents=True, exist_ok=True)
        self.yaml_filename = Path(self.directory) / Path(f"{self.filename_pattern}.yml")
        self.data_filename = Path(self.directory) / Path(f"{self.filename_pattern}.csv")
        self.info_filename = Path(self.directory) / Path(f"{self.filename_pattern}_info.txt")
        self.plot_filename = Path(self.directory) / Path(f"{self.filename_pattern}_plot.png")

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

    def process_jobs(self):
        # Parse the qstat file and create a dictionary object
        job_dict = self.qstat_parser()

        # Filter and keep only the selected jobs and then create a YAML file
        kept_jobs, kept_dict = self.filter_jobs(job_dict)
        print(f'jobs: {kept_jobs}')

        # if the yaml file doesnt exist then update the jobs and dump the qstat data
        if not self.yaml_filename.is_file() or self.watch is True:
            self.jobs = kept_jobs
            with open(self.yaml_filename, 'w') as yf:
                yaml.dump(kept_dict, stream=yf, default_flow_style=False)
        # If the yaml file is empty, then overwrite it.
        else:
            with open(self.yaml_filename, 'r') as yf:
                test = yaml.load(yf)
                if not test:
                    with open(self.yaml_filename, 'w') as yf2:
                        yaml.dump(kept_dict, stream=yf2, default_flow_style=False)

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
                            pass
                            #print("New Job or end of file!")
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

    def get_qstat_data(self):
        if not self.yaml_filename.is_file() or self.watch is True:
            self.process_jobs()
            with open(self.yaml_filename, 'r') as yf:
                jobs_dict = yaml.load(yf)
        else:
            with open(self.yaml_filename, 'r') as yf:
                test = yaml.load(yf)
                if not test:
                    self.process_jobs()
        with open(self.yaml_filename, 'r') as yf2:
            jobs_dict = yaml.load(yf2)
        return jobs_dict

    def get_dicts(self, python_datetime=None):
        jobs_dict = self.get_qstat_data()
        df = OrderedDict()
        master_dict = OrderedDict()
        info_dict = OrderedDict()
        data_dict = OrderedDict()
        for job in jobs_dict.keys():
            row = OrderedDict()
            _job = OrderedDict()
            row = jobs_dict[job]
            _job[job] = row
            df.update(_job)
        # Rework this
        for job in df.keys():
            var_dict = OrderedDict()
            # PBS Environment Variables
            for var in df[job]['Variable_List'].keys():
                var_dict[var] = [df[job]['Variable_List'][var]]
            info_dict[job] = var_dict
            data_dict[job] = OrderedDict()
            if python_datetime:
                info_dict[job]["datetime"] = [python_datetime]
                data_dict[job]["datetime"] = [python_datetime]

            for keyword in df[job].keys():
                if keyword in self.__static_kw:
                    if keyword != "Variable_List":
                        if keyword in self.__job_time_kw:
                            info_dict[job][keyword] = [str(parser.parse(df[job][keyword]))]
                        else:
                            info_dict[job][keyword] = [df[job][keyword]]
                elif keyword in self.__dynamic_kw:
                    data_dict[job][keyword] = [df[job][keyword]]

        master_dict["info"] = info_dict
        master_dict["data"] = data_dict
        return master_dict

    def get_dataframes(self, python_datetime=None):
        master_dict = self.get_dicts(python_datetime=python_datetime)
        master_df = OrderedDict()
        for key in master_dict.keys():
            if len(self.jobs) > 1:
                master_df[key] = pd.DataFrame.from_dict(master_dict[key])
            else:
                for job in master_dict[key].keys():
                    master_df[key] = pd.DataFrame.from_dict(dict(master_dict[key][job]))

        return master_df

    def get_info(self, data_frame=False, python_datetime=None):
        if data_frame:
            _data = self.get_dataframes(python_datetime=python_datetime)
        else:
            _data = self.get_dicts(python_datetime=python_datetime)
        return _data["info"]

    def get_data(self, data_frame=False, python_datetime=None):
        if data_frame:
            _data = self.get_dataframes(python_datetime=python_datetime)
        else:
            _data = self.get_dicts(python_datetime=python_datetime)
        return _data["data"]

    def filter_jobs(self, job_dict):
        kept_jobs = []
        print(len(self.users))
        print(len(self.jobs))
        print(self.jobs)
        if len(self.jobs) != 0 or len(self.users) != 0:
            print('if')
            for j in job_dict.keys():
                if len(self.users) > 0:
                    if job_dict[j]["Variable_List"]["PBS_O_LOGNAME"] in self.users:
                        print(j)
                        kept_jobs.append(j)
                elif len(self.jobs) > 0:
                    if job_dict[j]["Job_Id"] in self.jobs:
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
        with open(self._yaml_config, 'r') as yc:
            _checker_dict = yaml.load(yc)
            _checker_list = []
            _checker_list.append('Job Id')
            _checker_list = _checker_list + list(_checker_dict['Job Id'].keys())
            _checker_list = _checker_list + list(_checker_dict['Job Id']['Variable_List'].keys())
        _data_index_list = list(data.index)
        diff = list(set(_data_index_list) - set(_checker_list))
        if len(diff) != 0:
            with open('checker_log.log', 'a') as cl:
                cl.write('There are unresolved qstat keywords: %s\nAdd them to the qstat_dict.yml file\n\n' % diff)
        if file.is_file():
            file_df = pd.read_csv(file, index_col=False)
            print(f'fdf{file_df}')
            print(f'd{data}')
            updated_df = pd.concat([file_df, data])
            updated_df.to_csv(file, index=False, index_label=False)
        else:
            data.to_csv(file, index=False, index_label=False)

    def watch_jobs(self):
        self.parse_qstat_data()
        self.process_jobs()
        if len(self.jobs) > 1:
            kw_dict = {}
            kw_dict["jobs"] = self.jobs
            kw_dict["kwargs"] = self._get_subset_kwargs(skipped_kwargs=["jobs", "directory", "qwatch", "watch", "_yaml_config",
                                                                        "info_filename", "plot_filename", "qstat_filename",
                                                                        "data_filename", "yaml_filename", "users",
                                                                        "filename_pattern"])
            kw_dict["sleeper"] = 5
            kw_dict["directory"] = self.directory
            with open('temp_yaml.yml', 'w') as ty:
                yaml.dump(kw_dict, stream=ty, default_flow_style=False)

            qstat = subprocess.Popen('python3.6 waiter.py -yamlfile temp_yaml.yml', stderr=subprocess.PIPE,
                                     stdout=subprocess.PIPE, shell=True,
                                     encoding='utf-8', universal_newlines=False)
            out = qstat.stdout.read()
            error = qstat.stderr.read()
            print(f'out: {out}')
            print(f'error: {error}')
        elif len(self.jobs) == 1:
            self._watch(datetime.now())

    def _watch(self, python_datetime=None, first_time=True):
        """Wait until a job finishes and get updates."""

        job_dict = self.full_workflow(parse=True, process=True, data=True, metadata=False)

        if job_dict:
            data = self.get_data(data_frame=True, python_datetime=python_datetime)
            self.update_csv(file=self.data_filename, data=data)
            print(f"Updated qstat data for {job_id}")

            if job_dict[self.jobs[0]]['job_state'] == 'Q':
                sleep(self.sleeper)
                self._watch(datetime.now(), first_time=True)
            elif job_dict[self.jobs[0]]['job_state'] == 'R':
                # Create the static data file on the first instance of a running job
                if first_time:
                    info = self.get_info(data_frame=True, python_datetime=python_datetime)
                    self.update_csv(file=self.info_filename, data=info)
                sleep(self.sleeper)
                self._watch(datetime.now(), first_time=False)

        return f'Finished {self.jobs[0]}'

    def plot_memory(self):
        for job in self.jobs:
            _watch = Qwatch(directory=job, jobs=job)
            data = pd.read_csv(self.data_filename, index_col=False)
            info = pd.read_csv(self.info_filename, index_col=False).to_dict("records")[0]
            "%Y-%m-%d %H:%M:%S"

