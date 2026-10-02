# Use the upstream Project/Manifest, instantiated during the container build.
# Runtime must work without network access or writes to a user's home.
using ROMEO, MriResearchTools, ArgParse
println(unwrapping_main(ARGS))
