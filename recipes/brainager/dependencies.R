# 'getopt' is required for optparse
install.packages("https://cran.r-project.org/src/contrib/Archive/getopt/getopt_1.20.2.tar.gz", repos=NULL, type="source")

# 'glue', 'magrittr', 'stringi' are required for stringr
install.packages("https://cran.r-project.org/src/contrib/Archive/glue/glue_1.3.0.tar.gz", repos=NULL, type="source")
install.packages("https://cran.r-project.org/src/contrib/Archive/magrittr/magrittr_1.5.tar.gz", repos=NULL, type="source")
install.packages("https://cran.r-project.org/src/contrib/Archive/stringi/stringi_1.1.7.tar.gz", repos=NULL, type="source")

# 2. Install 'optparse' (Version 1.6.0 is compatible with R 3.4.4)
install.packages("https://cran.r-project.org/src/contrib/Archive/optparse/optparse_1.6.0.tar.gz", repos=NULL, type="source")

# 3. Install 'stringr' (Version 1.3.1 is compatible with R 3.4.4)
install.packages("https://cran.r-project.org/src/contrib/Archive/stringr/stringr_1.3.1.tar.gz", repos=NULL, type="source")

# Install 'kernlab' for compatiblity reasons directly from URL
install.packages("http://cran.r-project.org/src/contrib/Archive/kernlab/kernlab_0.9-27.tar.gz", repos = NULL, type = "source")

# Install 'caret' (Version 6.0-79 is compatible with R 3.4.4)
install.packages("https://cran.r-project.org/src/contrib/Archive/caret/caret_6.0-79.tar.gz", repos=NULL, type="source")

# Install the rest:
packages_to_install <- c(
  "proxy", 
  "iterators", 
  "Rcpp", 
  "data.table", 
  "e1071", 
  "foreach", 
  "ModelMetrics", 
  "plyr", 
  "pROC", 
  "reshape2", 
  "RNifti"
)

# 2. Loop through the list and install if missing
for (pkg in packages_to_install) {
  if(pkg %in% rownames(installed.packages()) == FALSE) {
    print(paste("Installing", pkg, "..."))
    install.packages(pkg, repos = "http://cran.us.r-project.org")
  }
}

# 3. Check that all packages are installed correctly
for (pkg in packages_to_install) {
  if(pkg %in% rownames(installed.packages()) == FALSE) {
    stop(paste("Package", pkg, "failed to install."))
  } else {
    print(paste("Package", pkg, "is installed."))
  }
}