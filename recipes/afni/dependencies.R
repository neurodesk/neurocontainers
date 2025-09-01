# R Script to install the dependencies
if("data.table" %in% rownames(installed.packages()) == FALSE) {install.packages("data.table", repos = "http://cran.us.r-project.org")}
if("cmdstanr" %in% rownames(installed.packages()) == FALSE) {install.packages("cmdstanr", repos = "https://stan-dev.r-universe.dev")}
cmdstanr::install_cmdstan(dir = "/opt/", cores = 2)
if("lme4" %in% rownames(installed.packages()) == FALSE) {install.packages("lme4", repos = "http://cran.us.r-project.org")}
if("lmerTest" %in% rownames(installed.packages()) == FALSE) {install.packages("lmerTest", repos = "http://cran.us.r-project.org")}
if("phia" %in% rownames(installed.packages()) == FALSE) {install.packages("phia", repos = "http://cran.us.r-project.org")}
if("ggplot2" %in% rownames(installed.packages()) == FALSE) {install.packages("ggplot2", repos = "http://cran.us.r-project.org")}
if("ggridges" %in% rownames(installed.packages()) == FALSE) {install.packages("ggridges", repos = "http://cran.us.r-project.org")}
if("dplyr" %in% rownames(installed.packages()) == FALSE) {install.packages("dplyr", repos = "http://cran.us.r-project.org")}
if("tidyr" %in% rownames(installed.packages()) == FALSE) {install.packages("tidyr", repos = "http://cran.us.r-project.org")}
if("scales" %in% rownames(installed.packages()) == FALSE) {install.packages("scales", repos = "http://cran.us.r-project.org")}
