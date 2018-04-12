# qwatch
qwatch is a command line utility for monitoring PBS jobs on the MCSR (and perhaps similar systems)

## Outline
* Look up qstat commands for inspiration
* Command line development:
    * _--user_ option displays info about all of the jobs from a specific user/list of users
    * _--job_ option displays info about a specific job/list of jobs
    * _--watch_ argument watches the jobs of interest over time
    * _--output_ option is a filename for the output
    * _--data_file_ option is a file name for the data monitoring
    * _--plot_data_ argument plots the data in the data_file.  (requires the --data_file_ option)
    * _--notify_ option is used to notify the user by email or by slack.
* Future development:
    * --database option for naming a new or old database for storing job data

