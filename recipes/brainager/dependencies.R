# R Script to install the dependencies
# 1. Define the URL for the compatible version (0.9-27)
package_url <- "http://cran.r-project.org/src/contrib/Archive/kernlab/kernlab_0.9-27.tar.gz"

# 2. Install directly from that URL
install.packages(package_url, repos = NULL, type = "source")

if("RNifti" %in% rownames(installed.packages()) == FALSE) {install.packages("RNifti", repos = "https://cran.us.r-project.org")}
if("stringr" %in% rownames(installed.packages()) == FALSE) {install.packages("stringr", repos = "http://cran.us.r-project.org")}
