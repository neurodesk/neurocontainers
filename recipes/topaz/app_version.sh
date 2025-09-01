
# app_version.txt - print the version of the application
# ======================================================
#
# this script should print out the version of the software in order to verify that the correct version is installed
# please retrieve the version by running one of the executables with a correct flag rather than simply hard-coding it. This ensures that the executable is of the correct version
# The Neurodesktop administrator will run it before every release of Neurodesktop.
# If the test return a version number that diverges from the expected one, the administrator will try to fix the problem (possible contacting you about it), or alternatively, mark the container as faulty in the specific Neurodesktop release.
#
# The script:
# - should return exit code 0 and print the version of the package (in exactly the same format used in Neurodesk) to stdout
# - should return 1 exit code and print "N/A" to stderr if the container developer hasn't included this functionality yet (that will be the default if the script provided in the template has not been modified)
# - should return non-zero exit code if the version cannot be retrieved, and an error message (if available) should be printed to stderr
#
# Notide that any other stderr or stdout will be captured when running the script and provided to the administrator as part of the testing. That said,
# given that many applications are tested, the administrator may not monitor the output and rely primarily on the exit code.
#
# The script must be a bash script, as it will be executed using 'bash -e /neurodesk/app_version.sh'. 
# The '-e' flag indicates to bash that if there is an error in the script, it should quit immediately with the non-zero exit code of the error rather than the bash default of continuing script execution after an error. This way errors in the script won't be overlooked during testing (the script continuing and returning zero exit code on termination, the administrator might not notice the error message).
#
# Notice that the script can also be executed by users directly by running /neurodesk/app_version.sh within the container. This can be used to double-check that the application works properly in the specific environment used by the user (although Singularity containers are supposed to run identically regardless of the execution environment, there are some exceptions).
#

####################################################################
# Uncomment the line below when your version test script is complete
#####################################################################
echo 'N/A' 1>&2; exit 1

####################################################################################
# The commands below provide an example for a version printing script. Please edit as necessary
#
# After the container is incorporated into Neurocontainers and being built by the CI, 
# convert it into a local sif file using the /neurocommand/local/fetch_and_run.sh command provided 
# in the "New container ..." issue confirming the container was built.
#
# If you see that the container runs successfully as a sif file, run the following commands to verify your test scripts return the appropriate output
# when being called using a singularity exec command (using your package NAME, VERSION, and BUILDDATE):
# 
# singularity --silent exec --pwd /tmp /neurocommand/local/containers/NAME_VERSION_BUILDDATE/NAME_VERSION_BUILDDATE.simg /bin/bash -e /neurodesk/app_version.sh 1>stdout 2>stderr
# echo 'EXIT CODE: '$?
# echo 'STDOUT:'
# cat stdout
# echo 'STDERR:'
# cat stderr
#
####################################################################################

EXEC='process'                           # executable of app
ARGUMENTS='-v'		         	 # arguments for executable

# an example for the version number being printed by the executable in the second line of output
if "$EXEC" ${ARGUMENTS} | head -2 | tail -1
then
    a=1 # empty command, just so I'll have a then clause. Cannot run mfcsc without if, because then will exit without explaining what was the problem
else
    exit_code=$?
    echo 'app_version.sh: executing '"$EXEC"' returned an error. Return non-zero exit code' 1>&2
    exit "$exit_code"
fi
