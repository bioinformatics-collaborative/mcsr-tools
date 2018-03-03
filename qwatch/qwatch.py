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
def qwatch(user, job, watch, outfile, save_data, data_filename, plot, notify):
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
        tf.truncate()
        yaml.dump(kept_dict, stream=tf)
        if watch:
            for job in kept_jobs:
                pass
                # Monitor jobs here


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
    return(mast_dict)


def wait_on_job_completion(self, job_id):
    """Use Qstat to monitor your job."""
    # TODO Allow either slack notifications or email or text.
    qwatch = Qstat().watch(job_id)
    if qwatch == 'Job id not found.':
        self.sgejob_log.info('%s has finished.' % job_id)
        sleep(30)
    elif qwatch == 'Waiting for %s to start running.' % job_id:
        self.sgejob_log.info('%s is queued to run.' % job_id)
        self.sgejob_log.info('Waiting for %s to start.' % job_id)
        sleep(30)
        self.wait_on_job_completion(job_id)
    elif qwatch == 'Waiting for %s to finish running.' % job_id:
        self.sgejob_log.info('%s is running.' % job_id)
        self.sgejob_log.info('Waiting for %s to finish.' % job_id)
        sleep(30)
        self.wait_on_job_completion(job_id)
    else:
        self.wait_on_job_completion(job_id)


"""Access qstat information about SGE jobs."""
class Qstat(object):
    def __init__(self):
        """Initialize class."""
        _username = getpass.getuser()
        self.username = _username
        self.split_regex = re.compile(r'\s+')

    def qstatinfo(self, qstat_path='qstat'):
        """Retrieve qstat output."""
        try:
            qstatinfo = subprocess.check_output([qstat_path])
        except subprocess.CalledProcessError as cpe:
            return_code = 'qstat returncode: %s' % cpe.returncode
            std_error = 'qstat standard output: %s' % cpe.stderr
            print(return_code + '\n' + std_error)
        except FileNotFoundError:
            raise FileNotFoundError('qstat is not on your machine.')

        jobs = self._output_parser(qstatinfo)

        return jobs

    def _output_parser(self, output):
        """Parse output from qstat pbs commandline program.
        Returns a list of dictionaries for each job.
        """
        lines = output.decode('utf-8').split('\n')
        del lines[:5]
        jobs = []
        for line in lines:
            els = self.split_regex.split(line)
            try:
                j = {"job_id": els[0], "name": els[1], "user": els[2], "elapsed_time": els[3],
                     "status": els[4], "queue": els[5]}
                jobs.append(j)

            except IndexError:
                pass

        return jobs

    def all_job_ids(self):
        """Retrieve a list of all jobs running or queued."""
        jobs = self.qstatinfo()
        ids = [j['job_id'] for j in jobs]
        return ids

    def all_running_jobs(self):
        """Retrieve a list of running jobs."""
        jobs = self.qstatinfo()
        ids = [j['job_id'] for j in jobs if j['status'] == 'R']
        return ids

    def all_queued_jobs(self):
        """Retrieve a list of queued jobs."""
        jobs = self.qstatinfo()
        ids = [j['job_id'] for j in jobs if j['status'] == 'Q']
        return ids

    def myjobs(self):
        """Retrieve a list of all the current user's jobs."""
        jobs = self.qstatinfo()
        ids = [j['job_id'] for j in jobs if j['user'] == self.username]
        if len(ids) < 1:
            return 'You have no jobs running or queued.'
        else:
            rids = [j['job_id'] for j in jobs if j['user'] == self.username
                    and j['status'] == 'R']
            qids = [j['job_id'] for j in jobs if j['user'] == self.username
                    and j['status'] == 'Q']
            return 'Running jobs: %s\nQueued jobs: %s' % (rids, qids)

    def watch(self, job_id):
        """Wait until a job or list of jobs finishes and get updates."""
        jobs = self.qstatinfo()
        rids = [j['job_id'] for j in jobs if j['user'] == self.username
                and j['status'] == 'R']
        qids = [j['job_id'] for j in jobs if j['user'] == self.username
                and j['status'] == 'Q']
        if job_id in qids:
            yield 'Waiting for %s to start running.' % job_id
            self.watch(job_id)
        elif job_id in rids:
            yield 'Waiting for %s to finish running.' % job_id
            self.watch(job_id)
        else:
            return 'Job id not found.'


if __name__ == '__main__':
    qwatch()