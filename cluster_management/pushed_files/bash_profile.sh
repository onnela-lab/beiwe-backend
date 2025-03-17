#!/bin/bash

# ~/.profile: executed by the command interpreter for login shells.
# This file is not read by bash(1), if ~/.bash_profile or ~/.bash_login
# exists.
# see /usr/share/doc/bash/examples/startup-files for examples.
# the files are located in the bash-doc package.

# the default umask is set in /etc/profile; for setting the umask
# for ssh logins, install and configure the libpam-umask package.
#umask 022

# enable pyenv cleanly
if test -f "/home/ubuntu/.pyenv/versions/beiwe/bin/python"; then
    export PATH="$HOME/.pyenv/bin:$PATH"
    eval "$(pyenv init --path)"
    eval "$(pyenv virtualenv-init -)"

    # set the global pyenv environment to this at every login
    pyenv global beiwe
fi

#type sudo less
alias apt="sudo apt"

#update aliases
alias update-commandline='cp ~/beiwe-backend/cluster_management/pushed_files/bash_profile.sh ~/.profile; cp ~/beiwe-backend/cluster_management/pushed_files/.inputrc ~/.inputrc'
alias update-pip='pip uninstall forest -y; pip install --upgrade pip setuptools wheel; pip install -r requirements.txt'

#Get file sizes of current folder
alias duu="sudo du -d 1 -h | sort -h"

#Swap managment
alias createswap="sudo fallocate -l 4G /swapfile; sudo chmod 600 /swapfile; sudo mkswap /swapfile; sudo swapon /swapfile; swapon -s"
alias deleteswap="sudo swapoff /swapfile; sudo rm /swapfile"

#Bash Utility
alias sudo="sudo " # allows the use of all our aliases with the sudo command
alias watch="watch "
alias n='nano -c' # nano with syntax highlighting
alias no="nano -Iwn" # nano in overwrite mode to allow for quick pasted overwrites
alias sn='sudo nano -c'
alias sno="sudo nano -Iwn"
alias ls='ls --color=auto' # make ls not suck
alias la='ls -A'
alias ll='ls -lh'
alias lg='ls -Alh | grep -i '
alias lh='ls -lhX --color=auto'
alias lll="du -ah --max-depth=0 --block-size=MB --time * | sort -nr"
alias slll="sudo du -ah --max-depth=0 --block-size=MB --time * | sort -nr"
alias grep='grep --color=auto' # make grep not suck
alias g='grep -i' # single letter for case insensitive grep
alias u="cd .." # navigate directories.
alias uu="cd ../.."
alias uuu="cd ../../.."

#Tools with command line config
alias htop="htop -d 5"
alias nload="nload -a 5 -i 80000 -o 80000"
alias df="df -h"

#Git
alias s='git status'
alias d='git diff'
alias gd='git diff'
alias dw='git diff -w'
alias gs='git diff --stat'
alias pull="git pull --ff-only"
alias purr="git pull --rebase"
alias checkout="git checkout"
alias gct="git checkout"


#File locations
alias b='cd ~/beiwe-backend/'
alias beiwe='cd ~/beiwe-backend/'

#Supervisord (data processing)
alias processing-start="supervisord"
alias processing-stop="killall supervisord > /dev/null 2>&1"
alias processing-restart="pkill -HUP supervisord 2> /dev/null"

#Watch Logs Live
alias log='tail -f /home/ubuntu/celery*.log /home/ubuntu/supervisor.log'
alias logs='logs'
alias logd='tail -f /home/ubuntu/supervisor.log'

#Configuration files
alias conf='sudo nano ~/beiwe-backend/config/settings.py'
alias settings='conf'
alias superconf='sudo nano /etc/supervisord.conf'

#Developer tools
alias db="cd ~/beiwe-backend/; python manage.py shell_plus"
alias ipy="ipython"
alias manage="python manage.py"
alias shell="python manage.py shell_plus"
alias showmigrations='manage showmigrations'
alias ag="clear; printf '_%.0s' {1..100}; echo ''; echo 'Silver results begin here:'; ag --column"
alias pyc='find . -type f -name "*.pyc" -delete -print'

# function that makes running a python script in the background really easy
function run {
    nohup python -u $1 > ~/$1_log.out &
}

# need to run something every second? This is your friend.
function runloop () {
    while true; do
        $*;
        sleep 1;
    done
}

# function to backup a postgres database, prints directions if .pgpass is not found.
function backup () {
    # parameter order: username, host, then it will prompt for password
    if [ -f ~/.pgpass ]
    then
        # this is at least a hundred times faster than the django dumpdata command
        pg_dump -F d -Z 9 -f /home/ubuntu/beiwe-backend/pg_dump --username=$1 --dbname=$2 --host=$3 --verbose
    else
        echo you have not created a ~/.pgpass file.
        echo pg_dumps password prompt does not like pasting, so use .pgpass.
        echo it is a text file of the form \"hostname:port:database:username:password\"
        echo the default postgres port is 5432
        echo Dont forget to include username, dbname, and host as parameters to this helper function when you run it.  They are required.
    fi
}

## Environment config ##

# uncomment for a colored prompt, if the terminal has the capability; turned
# off by default to not distract the user: the focus in a terminal window
# should be on the output of commands, not on the prompt
force_color_prompt=yes

# enable color support of ls and also add handy aliases
if [ -x /usr/bin/dircolors ]; then
    test -r ~/.dircolors && eval "$(dircolors -b ~/.dircolors)" || eval "$(dircolors -b)"
    alias ls='ls --color=auto -h'
    alias grep='grep --color=auto'
    alias fgrep='fgrep --color=auto'
    alias egrep='egrep --color=auto'
fi


# if running bash
if [ -n "$BASH_VERSION" ]; then
    # include .bashrc if it exists
    if [ -f "$HOME/.bashrc" ]; then
    . "$HOME/.bashrc"
    fi
fi

# set PATH so it includes user's private bin if it exists
if [ -d "$HOME/bin" ] ; then
    PATH="$HOME/bin:$PATH"
fi

#makes the current git branch visible (on the command line)
function parse_git_branch () {
       git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/ (\1)/'
}

RED="\[\033[0;31m\]"
YELLOW="\[\033[0;33m\]"
GREEN="\[\033[0;32m\]"
NO_COLOUR="\[\033[0m\]"
PS1="$GREEN\u$NO_COLOUR:\w$YELLOW\$(parse_git_branch)$NO_COLOUR\$ "

echo
echo "There are a number of bash aliases and functions to assist you in your work, type 'cat ~/.profile' to see them all."
echo

# update the bash profile every time the user logs in from the version in the repository.
update-commandline
