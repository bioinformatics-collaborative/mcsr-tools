import os
import asyncio
from pathlib import Path
from time import sleep, time
from datetime import datetime
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


async def _async_watch(job_id, directory, sleeper=120, python_datetime=None, **kwargs):
    """Wait until a job or list of jobs finishes and get updates."""

    watch_one = qwatch.Qwaiter(jobs=[job_id], directory=directory,  watch=True, users=None, **kwargs)
    job_dict = watch_one.full_workflow(parse=True, process=True, data=True, metadata=False)

    if job_dict:
        md = watch_one.get_metadata(data_frame=True, python_datetime=python_datetime)
        ev = watch_one.get_pbs_env(data_frame=True, python_datetime=python_datetime)
        pl = watch_one.get_plot_data(data_frame=True, python_datetime=python_datetime)
        watch_one.update_csv(file=watch_one.metadata_filename, data=md)
        watch_one.update_csv(file=watch_one.pbs_env_filename, data=ev)
        watch_one.update_csv(file=watch_one.plot_md_filename, data=pl)
        print(f"Updated qstat data for {job_id}")

        if job_dict[job_id]['job_state'] == 'Q':
            await asyncio.sleep(sleeper)
            await _async_watch(job_id, directory, sleeper, datetime.now(), **kwargs)
        elif job_dict[job_id]['job_state'] == 'R':
            await asyncio.sleep(sleeper)
            await _async_watch(job_id, directory, sleeper, datetime.now(), **kwargs)

    return f'Finished {job_id}'


def get_watcher_tasks(jobs, sleeper, home_dir, **kwargs):
    python_datetime = datetime.now()
    tasks = [asyncio.ensure_future(_async_watch(job_id=job, directory=home_dir/Path(job), sleeper=sleeper,
                                                python_datetime=python_datetime, **kwargs)) for job in jobs]
    return tasks


ioloop = asyncio.get_event_loop()
tasks = get_watcher_tasks(jobs=_kwargs["jobs"], sleeper=_kwargs["sleeper"],
                          home_dir=_kwargs["directory"], **_kwargs["kwargs"])
ioloop.run_until_complete(asyncio.wait(tasks))
ioloop.close()
