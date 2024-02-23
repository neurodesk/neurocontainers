# R Script to install the dependencies
if("devtools" %in% rownames(installed.packages()) == FALSE) {install.packages("devtools")}
if("MRIcrotome" %in% rownames(installed.packages()) == FALSE) {devtools::install_github("Mouse-Imaging-Centre/MRIcrotome", upgrade_dependencies = FALSE)}
if("RMINC" %in% rownames(installed.packages()) == FALSE) {devtools::install_github("Mouse-Imaging-Centre/RMINC", upgrade_dependencies = FALSE)}
if("ProbBayes" %in% rownames(installed.packages()) == FALSE) {install.packages("ProbBayes")}
if("bayesplot" %in% rownames(installed.packages()) == FALSE) {install.packages("bayesplot")}
if("brms" %in% rownames(installed.packages()) == FALSE) {install.packages("brms")}
if("rstan" %in% rownames(installed.packages()) == FALSE) {install.packages("rstan")}
if("Rcpp" %in% rownames(installed.packages()) == FALSE) {install.packages("Rcpp")}
if("reticulate" %in% rownames(installed.packages()) == FALSE) {install.packages("reticulate")}
if("fastICA" %in% rownames(installed.packages()) == FALSE) {install.packages("fastICA")}
if("forge" %in% rownames(installed.packages()) == FALSE) {install.packages("forge")}
if("rmarkdown" %in% rownames(installed.packages()) == FALSE) {install.packages("rmarkdown")}
if("plotly" %in% rownames(installed.packages()) == FALSE) {install.packages("plotly")}
if("shiny" %in% rownames(installed.packages()) == FALSE) {install.packages("shiny")}
if("tidyverse" %in% rownames(installed.packages()) == FALSE) {install.packages("tidyverse")}
if("RColorBrewer" %in% rownames(installed.packages()) == FALSE) {install.packages("RColorBrewer")}
if("paletteer" %in% rownames(installed.packages()) == FALSE) {install.packages("paletteer")}
if("ggpubr" %in% rownames(installed.packages()) == FALSE) {install.packages("ggpubr")}
if("ggthemes" %in% rownames(installed.packages()) == FALSE) {install.packages("ggthemes")}
if("rbokeh" %in% rownames(installed.packages()) == FALSE) {remotes::install_github("bokeh/rbokeh")}
if("RcppCNPy" %in% rownames(installed.packages()) == FALSE) {install.packages("RcppCNPy")}
if("R.matlab" %in% rownames(installed.packages()) == FALSE) {install.packages("R.matlab")}
if("caret" %in% rownames(installed.packages()) == FALSE) {install.packages("caret", dependencies = TRUE)}
if("car" %in% rownames(installed.packages()) == FALSE) {install.packages("car")}
if("BayesFactor" %in% rownames(installed.packages()) == FALSE) {install.packages("BayesFactor", dependencies = TRUE)}
