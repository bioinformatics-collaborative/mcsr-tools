import os
from pathlib import Path
import click
import subprocess
import yaml
import random
from tempfile import TemporaryFile
from pathlib import Path
from time import sleep
import getpass
import re

@click.command()
@click.option('--user', default=None)  # Done
@click.option('--job', default=None)  # Done
@click.option('--infile', default=None)
@click.option('-watch', is_flag=True, default=False)
@click.option('--outfile', default=None)  # Done
@click.option('--data_filename', default="qwatch_data")
@click.option('-plot', is_flag=True, default=False)
@click.option('--notify', nargs=2)
def qwatch(user, job, infile, outfile, data_filename, watch, plot, notify):
    if not infile:
        cmd = "qstat -f"
        qstat = subprocess.Popen(cmd, stderr=subprocess.PIPE,
                                   stdout=subprocess.PIPE, shell=True,
                                   encoding='utf-8', universal_newlines=False)
        out = qstat.stdout.readlines()
        error = qstat.stderr.readlines()
        print(error)
    if outfile:
        yml_handle = open("%s.yml" % outfile, mode='w+')
        qs_handle = open("%s.qstat" % outfile, mode='w+')
    else:
        yml_handle = TemporaryFile(mode="w+")
        qs_handle = TemporaryFile("%s.qstat" % outfile, mode='w+')

    # stopped here need to rework
    with yml_handle as tf:
        print(out)
        tf.writelines(out)
        job_dict = qstat_parser(tf)
        print(job_dict)
        kept_jobs, kept_dict = filter_jobs(job_dict, user=user, job=job)
        tf.truncate()
        yaml.dump(kept_dict, stream=tf)
    if watch:
        df_list = []
        for job in kept_jobs:
            data_filename = "%s_%s.csv" % (data_filename, job.split(".")[0])
            df_list.append(data_filename)
            subprocess.Popen(["python3.6 watcher --job %s --data_filename %s" % (job, data_filename)])
    if plot:
        # Wait for data file and then plot and/or notify.
        for job, df in kept_jobs, df_list:
            if plot and notify:
                subprocess.Popen(["python3.6 waiter --job %s --plot %s --notify %s" % (job, df, notify)])
            elif plot:
                subprocess.Popen(["python3.6 waiter --job %s --plot %s" % (job, df)])
    elif notify:
        for job in kept_jobs:
            subprocess.Popen(["python3.6 waiter --job %s --notify %s" % (job, notify)])



@click.command()
@click.option('--users', default=[])  # Pattern Matching
@click.option('--jobs', default=[])  # Pattern Matching
@click.option('--metadata', default=[])  # Pattern Matching
@click.option('--email', nargs=2)  # Notification
@click.option('--infile', default=None)  # Data Parsing
@click.option('-watch', is_flag=True, default=False)  # Data Parsing
@click.option('-plot', is_flag=True, default=False)  # Output
@click.option('--filename_pattern', default=None)  # Output
@click.option('--directory', default='.') # Output
def _qwatch(users, jobs, metadata, email, infile, watch, plot, filename_pattern, directory):
    # Create a matching file name pattern if one is not already given
    if not filename_pattern:
        if len(users) == 1 and len(jobs) == 0:
            filename_pattern = f"{users[0]}_qstat_data"
        elif len(jobs) == 1 and len(users) == 0:
            filename_pattern = f"{jobs[0]}_qstat_data"
        else:
            current_user = os.getlogin()
            id = random.randint(10000, 99999)
            filename_pattern = f"{current_user}_{id}_qstat_data"

    # Create file names using the pattern
    Path(directory).mkdir(parents=True, exist_ok=True)
    yaml_filename = Path(directory) / Path(f"{filename_pattern}.yml")
    csv_filename = Path(directory) / Path(f"{filename_pattern}.csv")
    plot_filename = Path(directory) / Path(f"{filename_pattern}.png")
    # If there is an input file parse that, otherwise parse the qstat output
    if infile:
        qstat_filename = Path(infile)
    else:
        qstat_filename = Path(directory) / Path(f"{filename_pattern}.txt")
        cmd = "qstat -f"
        qstat = subprocess.Popen(cmd, stderr=subprocess.PIPE,
                                 stdout=subprocess.PIPE, shell=True,
                                 encoding='utf-8', universal_newlines=False)
        out = qstat.stdout.readlines()
        error = qstat.stderr.readlines()
        with open(qstat_filename, 'w') as qf:
            qf.writelines(out)
        print(error)

    # Parse the qstat file and create a dictionary object
    job_dict = qstat_parser(qstat_filename)
    print(job_dict)
    # Filter and keep only the selected jobs and then create a YAML file
    kept_jobs, kept_dict = filter_jobs(job_dict, users=users, jobs=jobs)
    with open(yaml_filename, 'w') as yf:
        yaml.dump(kept_dict, stream=yf)


class Qwatch(object):
    """
    A class for parsing "qstat -f" output on SGE systems for monitoring
    jobs and making smart decisions about resource allocation.
    """
    def __init__(self, jobs: list=None, metadata: str=None, email: str=None, infile: str=None, watch: (bool or None)=None, plot: (bool or None)=None,
                 filename_pattern: str=None, directory: str=None, users: list = list(os.getlogin()), cmd: str="qstat -f"):
        self.cmd = cmd
        self.users = users
        self.jobs = jobs
        self.metadata = metadata
        self.email = email
        self.infile = infile
        self.watch = watch
        self.plot = plot
        self.directory = directory
        self.filename_pattern = filename_pattern
        self.qstat_filename = Path()
        self.yaml_filename, self.csv_filename, self.plot_filename = Path(), Path(), Path()
        self._yaml_config = "qwatch/qstat_dict.yml"

    def initialize_data_files(self):

        if not self.filename_pattern:
            if len(self.users) == 1 and len(self.jobs) == 0:
                filename_pattern = f"{users[0]}_qstat_data"
            elif len(self.jobs) == 1 and len(self.users) == 0:
                filename_pattern = f"{jobs[0]}_qstat_data"
            else:
                current_user = os.getlogin()
                _id = random.randint(10000, 99999)
                filename_pattern = f"{current_user}_{_id}_qstat_data"
        self.filename_pattern = filename_pattern

        # Create file names using the pattern
        Path(self.directory).mkdir(parents=True, exist_ok=True)
        self.yaml_filename = Path(self.directory) / Path(f"{filename_pattern}.yml")
        self.csv_filename = Path(self.directory) / Path(f"{filename_pattern}.csv")
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

    def process_jobs(self):
        # Parse the qstat file and create a dictionary object
        job_dict = self.qstat_parser()
        print(job_dict)
        # Filter and keep only the selected jobs and then create a YAML file
        kept_jobs, kept_dict = self.filter_jobs(job_dict)
        with open(self.yaml_filename, 'w') as yf:
            yaml.dump(kept_dict, stream=yf)

    def qstat_parser(self):
        mast_dict = {}
        line_list = []
        line_flag = False
        sub_dict = {}
        job_count = 0
        keyword_flag = None
        jobid_flag = None
        space_flag = None
        tab_flag = None
        temp_dict = {}
        with open(self.qstat_filename, 'r') as qf:
            #print(qf.readlines())
            with open(self._yaml_config, 'r') as yf:
                qstat_keywords = yaml.load(yf)
                qstat_sentence = None
                cont_phrase = ""
                qstat_phrase = ""
                prev_item = None
                for item in qf.readlines():
                    if keyword_flag is False:
                        #print(last_item)
                        pass
                    if "Job Id" in item:
                        jobid_flag = True
                        job_count += 1
                        line_list = []
                        var_list = []
                        _ = item.split(": ")
                        main_key = "%s" % _[1].replace("\r\n", "")
                        main_key = main_key.replace("\n", "")
                        mast_dict[main_key] = {}
                        mast_dict[main_key]["Job Id"] = "%s" % _[1]
                    elif "    " in item and any(kw in item for kw in list(qstat_keywords["Job Id"].keys()) + list(qstat_keywords["Job Id"]["Variable_List"].keys())) or item == "\n":
                        #print(f'spaces:{item}')
                        item = item.replace("    ", "")
                        if tab_flag is True:
                            qstat_sentence = qstat_phrase + cont_phrase
                            tab_flag = False
                        elif qstat_phrase == prev_item:
                            qstat_sentence = qstat_phrase

                        if item != "\n":
                            qstat_phrase = item
                        else:
                            print("New Job or end of file!")

                    elif "\t" in item:
                        tab_flag = True
                        #print(f'tabs: {item}')
                        cont_phrase = cont_phrase + item

                    # For multi-line phrases
                    if tab_flag is False:
                        qstat_sentence = qstat_sentence.replace("\n\t", "")
                        print(f'sentence:  {qstat_sentence}')
                        cont_phrase = ""
                        tab_flag = None
                        qstat_list = qstat_sentence.split(" = ")
                        key = qstat_list[0]
                        value = qstat_list[1]
                        if key == "Variable_List":
                            value = value.split(",")
                            temp_dict = dict(var.split("=") for var in value)
                            for key, value in temp_dict.items():
                                if value[0] == "/" or value[0] == "\\":
                                    value = Path(value)
                                temp_dict[key] = value
                            mast_dict[main_key]["Variable_List"] = temp_dict
                            temp_dict = {}
                        else:
                            mast_dict[main_key][key] = qstat_sentence.replace('\n', '')

                    # For single line Phrases
                    elif qstat_sentence:
                        qstat_sentence = qstat_sentence.replace("\n\t", "")
                        print(f'sentence: {qstat_sentence}')
                        qstat_list = qstat_sentence.split(" = ")
                        key = qstat_list[0]
                        value = qstat_list[1]
                        mast_dict[main_key][key] = qstat_sentence.replace('\n', '')
                    qstat_sentence = None
                    prev_item = item
        return mast_dict

    # def qstat_parser(self):
    #     mast_dict = {}
    #     line_list = []
    #     line_flag = False
    #     sub_dict = {}
    #     job_count = 0
    #     with open(self.qstat_filename, 'r') as qf:
    #         for item in qf.readlines():
    #             print(f'item: {item}')
    #             item = item.rstrip("\n")
    #             item = item.rstrip("\r")
    #             if "Job Id" in item:
    #                 job_count += 1
    #                 line_list = []
    #                 _ = item.split(": ")
    #                 main_key = "%s" % _[1].replace("\r\n", "")
    #                 mast_dict[main_key] = {}
    #                 mast_dict[main_key]["Job Id"] = "%s" % _[1]
    #                 continue
    #             elif item == "\n":
    #                 continue
    #             else:
    #                 _ = item.split(" = ")
    #                 if item == ['']:
    #                     continue
    #                 #print(_)
    #                 if len(_) == 2:
    #                     key = _[0].replace(" ", "")
    #                     value = _[1].replace("\r\n", "")
    #                     sub_dict[key] = value
    #                     line_flag = False
    #                     mast_dict[main_key] = sub_dict
    #                 elif len(_) == 1:
    #                     prev_key = key
    #                     print(_)
    #                     line_flag = True
    #                     if len(line_list) == 0:
    #                         line_list.append(value)
    #                     line_list.append(_[0].replace("\r\n", "").replace(" ", "").replace("\t", ""))
    #                     #print(line_list)
    #                 if not line_flag and len(line_list) > 0:
    #                     print(f'line_list1: {line_list}')
    #                     line_string = ''.join(line_list)
    #                     print(f'line_string: {line_string}')
    #                     line_list = line_string.split(",")
    #                     print(f'line_list2: {line_list}')
    #                     print(f'key: {prev_key}')
    #                     print(f'main_key: {main_key}')
    #                     if prev_key == "Variable_List":
    #                         temp_dict = dict(var.split("=") for var in line_list)
    #                         print(f'temp_dict: {temp_dict}')
    #                         for key, value in temp_dict.items():
    #                             if value[0] == "/" or value[0] == "\\":
    #                                 value = Path(value)
    #                             temp_dict[key] = value
    #                         sub_dict[prev_key] = temp_dict
    #                     else:
    #                         sub_dict[prev_key] = line_list[0]
    #                     mast_dict[main_key] = sub_dict
    #                     line_list = []
    #                 continue
    #     return mast_dict

    def filter_jobs(self, job_dict):
        kept_jobs = []
        if len(self.jobs) != 0 or len(self.users) != 0:
            for j in job_dict.keys():
                if len(self.users) > 0:
                    if job_dict[j]["Job_Owner"] in self.users:
                        kept_jobs.append(j)
                elif len(self.jobs) > 0:
                    if job_dict[j]["Job_Name"] in self.jobs:
                        kept_jobs.append(j)
        else:
            kept_jobs += list(job_dict.keys())

        kept_jobs = list(set(kept_jobs))
        kept_dict = dict((k, job_dict[k]) for k in kept_jobs)
        return kept_jobs, kept_dict


if __name__ == '__main__':
    qwatch()