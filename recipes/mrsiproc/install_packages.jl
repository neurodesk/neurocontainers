import Pkg
ENV["JULIA_PKG_PRECOMPILE_AUTO"]=0
packages = ["MAT", "Comonicon"]
Pkg.add(packages)
