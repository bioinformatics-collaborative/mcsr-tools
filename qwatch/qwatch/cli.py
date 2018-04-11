import click
import os
from qwatch import Qwatch


@click.command()
@click.option('--jobs', '-j', multiple=True)
@click.option('--users', '-u', multiple=True)
@click.option('--email', '-e', default=None)
@click.option('--infile', '-in', default=None)
@click.option('--watch/--no-watch', default=False)
@click.option('--filename_pattern', default=None)
@click.option('--directory')
@click.option('--cmd')
@click.option('--sleeper')
@click.option('--notifier')
def qwatch(jobs: (list or str)=None, email: str=None, infile: str=None, watch: (bool or None)=None,
           filename_pattern: str=None, directory: str='.', users: (list or str) = os.getlogin(), cmd: str="qstat -f",
           sleeper: int=120):
    qwatch = Qwatch()
    pass


if __name__ == '__main__':
    qwatch()
