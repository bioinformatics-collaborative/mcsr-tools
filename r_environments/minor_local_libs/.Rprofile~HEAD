# Author: Rob Gilmore, Shaurita Hutchins, and Eric Vallender
#
#
# This .Rprofile is used to get a more specific local library
# path (.libPaths()) for even minor versions of R.  The local library
# path is where bioconductor/cran/github packages are stored in each
# users home directory.
#
# Keeping the local libraries seperate for each minor version of R 
# on the MCSR will keep pipelines from breaking because of dependency
# issues as well as others (see below).
#
#
# R - 3.4.3 is compiled with intel
# R - 3.4.4 is compiled with gcc

version_path <- sprintf("%s.%s", R.version$major, R.version$minor)
local_lib <- sprintf("~/R/%s-library/%s", R.version$platform, version_path)

if (!dir.exists(local_lib)) {
  dir.create(local_lib, recursive=TRUE)
}

.libPaths(local_lib)

repo = getOption("repos") 
repo["CRAN"] = "https://mirrors.nics.utk.edu/cran/"
options(repos = repo)
rm(repo)
rm(local_lib)
rm(version_path)