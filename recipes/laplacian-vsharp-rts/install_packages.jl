using Pkg
ENV["JULIA_PKG_PRECOMPILE_AUTO"]=0
Pkg.add("JSON")
Pkg.add(Pkg.PackageSpec(name="FFTW", version=v"1.8"))
Pkg.add(Pkg.PackageSpec(name="QSM", version=v"0.5.4"))
Pkg.add(Pkg.PackageSpec(name="NIfTI", version=v"0.6.0"))