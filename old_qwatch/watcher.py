import click
import subprocess
from qwatch.qwatch import qstat_parser
from tempfile import TemporaryFile
import time
import pandas as pd

@click.command()
@click.option("--job")
@click.option("--data_filename")
def watcher(job, data_filename):
    run_flag = True
    cput_limit, mem_limit, cpupercent, cput, mem, ncpus, vmem, walltime = [], [], [], [], [], [], [], []
    while run_flag:

        cmd = "qstat %s" % job
        qstat = subprocess.Popen(cmd, stderr=subprocess.PIPE,
                                   stdout=subprocess.PIPE, shell=True,
                                   encoding='utf-8', universal_newlines=False)
        out = qstat.stdout.readlines()
        error = qstat.stderr.read()
        if "Unknown Job Id" in error[0]:
            run_flag = False
        else:
            with TemporaryFile(mode="w+") as tf:
                tf.writelines(out)
                job_dict = qstat_parser(tf)
                cput_limit.append(job_dict["Resource_List.cput"])
                mem_limit.append(job_dict["Resource_List.mem"])
                cpupercent.append(job_dict["resources_used.cpupercent"])
                cput.append(job_dict["resources_used.cput"])
                mem.append(job_dict["resources_used.mem"])
                ncpus.append(job_dict["resources_used.ncpus"])
                vmem.append(job_dict["resources_used.vmem"])
                walltime.append(job_dict["resources_used.walltime"])
            time.sleep(120)
    data_dict = {"CPU Time Limit": cput_limit,
                 "Memory Limit": mem_limit,
                 "CPU Percent": cpupercent,
                 "CPU Time": cput,
                 "Memory Used": mem,
                 "Number of CPUs": ncpus,
                 "Virtual Memory Used": vmem,
                 "Walltime": walltime
                 }
    df = pd.DataFrame(data_dict)
    df = df.set_index("Walltime")

    df.to_csv(data_filename)