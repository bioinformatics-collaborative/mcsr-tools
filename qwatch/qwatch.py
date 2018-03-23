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


def filter_jobs(job_dict, users, jobs):
    kept_jobs = []
    if len(jobs) != 0 or len(users) != 0:
        for j in job_dict.keys():
            if len(users) > 0:
                if job_dict[j]["Job_Owner"] in users:
                    kept_jobs.append(j)
            elif len(jobs) > 0:
                if job_dict[j]["Job_Name"] in jobs:
                    kept_jobs.append(j)
    else:
        kept_jobs += list(job_dict.keys())

    kept_jobs = list(set(kept_jobs))
    kept_dict = dict((k, job_dict[k]) for k in kept_jobs)
    return kept_jobs, kept_dict


def qstat_parser(qstat_filename):
    mast_dict = {}
    line_list = []
    line_flag = False
    sub_dict = {}
    job_count = 0
    with open(qstat_filename, 'r') as qf:
        for item in qf.readlines():
            print(item)
            item = item.rstrip("\n")
            item = item.rstrip("\r")
            if "Job Id" in item:
                job_count += 1
                if job_count > len(mast_dict.keys()) and job_count > 1:
                    mast_dict[main_key] = sub_dict
                    mast_dict[main_key]["Variable_List"] = dict(var.split("=") for var in mast_dict[main_key]["Variable_List"])
                _ = item.split(": ")
                main_key = "%s" % _[1].replace("\r\n", "")
                mast_dict[main_key] = {}
                mast_dict[main_key]["Job Id"] = "%s" % _[1]
                continue
            else:
                _ = item.split(" = ")
                if len(_) == 2:
                    key = _[0].replace(" ", "")
                    value = _[1].replace("\r\n", "")
                    sub_dict[key] = value
                    line_flag = False
                elif len(_) == 1:
                    k = key
                    v = value
                    line_flag = True
                    if len(line_list) == 0:
                        line_list.append(value)
                    line_list.append(_[0].replace("\r\n", "").replace(" ", ""))
                if not line_flag and len(line_list) > 0:
                    line_string = ''.join(line_list)
                    line_list = line_string.split(",")
                    sub_dict[k] = line_list
                    line_list = []
            mast_dict[main_key] = sub_dict
            temp_dict = dict(var.split("=") for var in mast_dict[main_key]["Variable_List"])
            for key, value in temp_dict.items():
                if value[0] == "/" or value[0] == "\\":
                    value = Path(value)
                temp_dict[key] = value
            mast_dict[main_key]["Variable_List"] = temp_dict
    return mast_dict


if __name__ == '__main__':
    qwatch()