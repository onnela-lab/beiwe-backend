#!/bin/bash


# if you provide a parameter to this script we just blindly run that script via the taskrunner
# without any extra checks or prompts and then exit with that status code.
if [ -n "$1" ]; then
    # (this is the hard-coded path to the beiwe python environment, which is not in the PATH, 
    # because we use pyenv to manage python versions.)
    /home/ubuntu/.pyenv/versions/beiwe/bin/python -u scripts/taskrunner.py $1 $2 $3 $4 $5 $6 $7 $8 $9
    exit $?  # exit with that status code
fi


echo
echo "This task runner will dispatch a script safely, running on the local machine with output redirected to a log file."
echo "Output from the script will immediately be followed, but you can exit the live follow at any time by pressing Ctrl+C."
echo "This action WILL NOT STOP THE SCRIPT, it will just stop following the output."
echo
echo "Available Scripts:"


# Only scripts that start with "script_that_" will be listed.
# Other scripts are for internal use
SCRIPTS=$(find "./scripts" -maxdepth 1 -type f -name "script_that_*.py")

# Dynamically filter scripts based on the exclusion list
select SCRIPT in $(basename -a $SCRIPTS); do
    if [ -n "$SCRIPT" ]; then
        break
    else
        echo "Invalid selection. Please try again."
    fi
done

while true; do
    SCRIPT_PARAM="${SCRIPT::-3}"
    echo "You have selected:"
    echo
    echo $SCRIPT_PARAM
    echo
    read -p "Was this the script you wanted to run? (y/n) " yn
    case $yn in
        [Yy]* ) 
            echo "Starting the script..."
            break;;
        [Nn]* ) 
            echo "Exiting..."
            exit;;
        * ) 
            echo "Please answer y or n.";;
    esac
done

# it is possible for the file to not be created by the time we start tailing it, so create it first.
LOG_FILE=${SCRIPT_PARAM}_$(date +"%Y-%m-%d_%H:%M:%S").log
touch $LOG_FILE

# run the script
# using nohup so it won't die when the terminal is closed
# with low priority
# in the background
# with output rediredected to a log file
# and wrapped in the time command to measure the execution time and resource usage
nohup time -v nice -19 python -u scripts/taskrunner.py $SCRIPT_PARAM >> $LOG_FILE 2>&1 &

echo
echo Following output of the log file, press Ctrl+C to stop - will not cancel the script.
echo
tail -f $LOG_FILE
