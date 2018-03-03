import os
from pathlib import Path
import click
import subprocess
import yaml
from tempfile import TemporaryFile
from time import sleep
import getpass
import re

@click.command()
@click.option('--user', default=None)  # Done
@click.option('--job', default=None)  # Done
@click.option('-watch', is_flag=True, default=False)
@click.option('--outfile', default=None)  # Done
@click.option('--data_filename', default="qwatch_data")
@click.option('-plot', is_flag=True, default=False)
@click.option('--notify', nargs=2)
def qwatch(user, job, watch, outfile, data_filename, plot, notify):
    cmd = "qstat -f"
    qstat = subprocess.Popen(cmd, stderr=subprocess.PIPE,
                               stdout=subprocess.PIPE, shell=True,
                               encoding='utf-8', universal_newlines=False)
    out = qstat.stdout.readlines()
    error = qstat.stderr.readlines()
    print(error)
    if outfile:
        output_handle = open("%s.yml" % outfile, mode='w+')
    else:
        output_handle = TemporaryFile(mode="w+")

    with output_handle as tf:
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


def filter_jobs(job_dict, user, job):
    kept_jobs = []
    for j in job_dict.keys():
        if user:
            if user in job_dict[j]["Job_Owner"]:
                kept_jobs.append(j)
        if job:
            if job in job_dict[j]["Job_Name"]:
                kept_jobs.append(j)
            elif job in j:
                kept_jobs.append(j)
        if not job and not user:
            kept_jobs.append(j)

    kept_jobs = list(set(kept_jobs))
    kept_dict = dict((k, job_dict[k]) for k in kept_jobs)
    return kept_jobs, kept_dict


def qstat_parser(qstat_stream):
    mast_dict = {}
    line_list = []
    line_flag = False
    sub_dict = {}
    job_count = 0

    for item in qstat_stream.readlines():
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