import os
import asyncio
from pathlib import Path
from time import sleep
import importlib.util
import argparse
import yaml
spec = importlib.util.spec_from_file_location(".", "./qwatch.py")
qwatch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qwatch)

parser = argparse.ArgumentParser(description='A job watcher.')

parser.add_argument('-yamlfile', action="store", dest="yamlfile")
_ = parser.parse_args()
yfile = _.yamlfile
with open(yfile, 'r') as yf:
    _kwargs = yaml.load(yf)


async def _async_watch_jobs(jobs, sleeper, **kwargs):

    # with tempfile.TemporaryDirectory() as tempdir:
    print(f'async_jobs:{jobs}')
    print(f'async_kwargs:{kwargs}')
    dir = Path(os.getcwd()) / Path('qwait_test')
    tasks = [asyncio.ensure_future(_async_watch(job_id=job, directory=dir, sleeper=sleeper, **kwargs))
             for job in jobs]
    await asyncio.wait(tasks)


async def _async_watch(job_id, directory, sleeper=120, **kwargs):
    """Wait until a job or list of jobs finishes and get updates."""
    directory = directory / Path(job_id)
    watch_one = qwatch.Qwaiter(jobs=[job_id], directory=directory, **kwargs)
    job_dict = watch_one.full_workflow(watch=True, parse=True, process=True, data=True, metadata=False)
    md = watch_one.get_metadata(watch_flag=True, data_frame=True)
    ev = watch_one.get_pbs_env(watch_flag=True, data_frame=True)
    tm = watch_one.get_time(watch_flag=True, data_frame=True)
    rs = watch_one.get_resources(watch_flag=True, data_frame=True)
    watch_one.update_csv(file=watch_one.metadata_filename, data=md)
    watch_one.update_csv(file=watch_one.vl_metadata_filename, data=ev)
    watch_one.update_csv(file=watch_one.time_metadata_filename, data=tm)
    watch_one.update_csv(file=watch_one.resource_metadata_filename, data=rs)

    if job_dict[job_id]['job_state'] == 'Q':
        yield 'Waiting for %s to start running.' % job_id
        sleep(sleeper)
        _async_watch(job_id, directory, sleeper, **kwargs)
    elif job_dict[job_id]['job_state'] == 'R':
        yield 'Waiting for %s to finish running.' % job_id
        sleep(sleeper)
        _async_watch(job_id, directory, sleeper, **kwargs)

ioloop = asyncio.get_event_loop()
ioloop.run_until_complete(_async_watch_jobs(jobs=_kwargs["jobs"], sleeper=_kwargs["sleeper"], **_kwargs["kwargs"]))
ioloop.close()
