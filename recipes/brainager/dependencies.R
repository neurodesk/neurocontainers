# Ubuntu supplies caret's dependencies built for the container's R version.
# Keep the archived versions used by the original prediction environment.
archives <- c(
  getopt = "1.20.2",
  glue = "1.3.0",
  magrittr = "1.5",
  stringi = "1.1.7",
  optparse = "1.6.0",
  stringr = "1.3.1",
  kernlab = "0.9-27",
  caret = "6.0-79"
)

for (pkg in names(archives)) {
  version <- archives[[pkg]]
  url <- sprintf("https://cran.r-project.org/src/contrib/Archive/%s/%s_%s.tar.gz",
                 pkg, pkg, version)
  install.packages(url, repos = NULL, type = "source")
  if (!requireNamespace(pkg, quietly = TRUE) ||
      packageVersion(pkg) != package_version(version)) {
    stop(paste("Failed to install", pkg, version))
  }
}

packages_to_install <- c(
  "proxy", "iterators", "Rcpp", "data.table", "e1071", "foreach",
  "ModelMetrics", "plyr", "pROC", "reshape2", "RNifti"
)
for (pkg in packages_to_install) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
}
for (pkg in c(names(archives), packages_to_install)) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(paste("Package", pkg, "failed to load."))
  }
}
