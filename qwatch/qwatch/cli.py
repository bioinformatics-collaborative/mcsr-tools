import click
from sys import platform
import os
from pkg_resources import resource_filename
from pathlib import Path
import random
from qwatch import Qwatch, NotRequiredIf, utils


@click.command()
@click.option('--jobs', '-j', cls=NotRequiredIf, not_required_if="users", multiple=True,
              help="Provide the name of a job(s) to watch. (qwatch -j 9001.sequoia -j 90002.sequoia")
@click.option('--users', '-u', cls=NotRequiredIf, not_required_if="jobs", multiple=True,
              help="Provide the name of a user(s) to watch. (qwatch -u bnp -u mh1 -u r9001")
@click.option('--email', '-e', default=None,
              help="Provide an email address for receiving data files or updates.")
@click.option('--slack', nargs=2, default=None,
              help="Provide slack details. (undeveloped)")
@click.option('--infile', '-in', default=None, type=click.Path(exists=True),
              help="Provide an input file to parse.  This does not watch a job, but it will parse qstat data for you.")
@click.option('--filename_pattern', default=None,
              help="Provide a naming schema for the output files:\n"
                   "The yaml file's path looks like <directory>/<filename_pattern>.yml\n"
                   "The data file's path looks like <directory>/<filename_pattern>.csv\n"
                   "The info file's path looks like <directory>/<filename_pattern>_info.txt\n"
                   "The plot file's path looks like <directory>/<filename_pattern>_plot.png")
@click.option('--directory', default=Path(f'qwatch{random.randint(9001, 10000)}'), show_default=True, # Over 9000!!
              help="Provide a custom output directory.  "
                   "qwatch will automatically create a directory with a random string of numbers.")
@click.option('--cmd', default="qstat -f", type=str, show_default=True,
              help="Provide an alternate command to parse.  "
                   "Currently, qstat -f output is the only thing that can be parsed with qwatch.")
@click.option('--sleeper', default=120, type=int, show_default=True,
              help="Provide a minimum number of seconds to wait in between each data point.  "
                   "This begins to vary as the time in between gets shorter, because of processing speeds."
                   "The default 120s minimum is recommended because qstat updates every 2 minutes.")
@click.option('--pip_config', '-pc', is_flag=True, flag_value=True, is_eager=True, default=False,
              help="In order to set up a local pip/pipenv executable with python3.6 (miniconda) use this flag.  "
                   "It will install pip/pipenv executables in ~/.local/bin, "
                   "and add this to the PATH in your .bash_profile.")
@click.option('--r_config', '-rc', is_flag=True, flag_value=True, is_eager=True, default=False,
              help="This will load the proper R environment with the LMOD command (module load R)."
                   "The default R is currently R/3.4.4 which has jpeg/png/tiff capabilities")
@click.option('--r_install', '-ri', is_flag=True, flag_value=True, is_eager=True, default=False,
               help="This will install relevant R dependencies for the plotting script.")
@click.option('--clean', is_flag=True, flag_value=True, is_eager=True, default=False,
              help="This will clean/move all of the files other than *.info, *.data, *.png in the output directory "
                   "to an archive folder.  Called on its own.")
@click.option('--clean_after', '-ca', is_flag=True, flag_value=True, default=False,
              help="This will clean directly after a qwatch monitors jobs")
@click.version_option(version='0.2.0', prog_name="qwatch")
def qwatch(**kwargs):

    if platform != "linux" and platform != "linux2":
        raise OSError("This script is meant for a Linux environment.")

    pip_config = kwargs.pop('pip_config', None)
    r_config = kwargs.pop('r_config', None)
    r_install = kwargs.pop('r_install', None)
    clean = kwargs.pop('clean', None)
    clean_after = kwargs.pop('clean_after', None)
    watcher = Qwatch(**kwargs)
    if clean:
        watcher.clean_output()
    elif r_config or r_install:
        print("Loading R 3.4.4.....")
        os.system("module load intel;module load R")
        if r_install:
            r_install_file = resource_filename(utils.__name__, 'install.R')
            print("Installing necessary packages...")
            os.system(f"Rscript {r_install_file}")
    elif pip_config:
        config_file = resource_filename(utils.__name__, 'config.sh')
        os.system(f". {config_file}")
        print("pip and pipenv have been configured to run locally for python3.6 (miniconda3).")
        print("Below are are the paths of your executables:")
        os.system("which pip pipenv python3.6")
    else:
        watcher.watch_jobs()
        if clean_after:
            watcher.clean_output()
