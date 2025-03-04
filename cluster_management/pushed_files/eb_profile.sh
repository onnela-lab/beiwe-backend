# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific aliases and functions

# enable the python venv, load the current environment variables for the ssh session.
source /var/app/venv/*/bin/activate 

`/opt/elasticbeanstalk/bin/get-config optionsettings | jq '."aws:elasticbeanstalk:application:environment"' | jq -r 'to_entries | .[] | "export \(.key)=\(.value)"'`

# cd into the current app directory
cd /var/app
cd /var/app/current


# location aliases
alias b='cd /var/app; cd /var/app/current'
alias conf='cd /etc/httpd/conf.d/elasticbeanstalk'


export EDITOR='nano'



echo '
Important log files and aliases:
loga:  /var/log/httpd/access_log /var/log/httpd/error_log   # httpd
logm:  /var/log/messages                                    # system messages
logu:  /var/log/web.stdout.log                              # gunicorn/django/gunicorn
logeb: /var/log/cfn-... /var/log/eb-...                     # AWS
logw - follows not the eb logs
log, logs - watch all logs
'

## platform log file notes
# eb-engine.log     # the useful one
# eb-cfn-init.log   # some output about deploys 1
# eb-publish.log    # some output about deploys 2

# cfn-hup.log       # "command processing is alive" (junk)
# cfn-init-cmd.log  # real output from deployment operations
# cfn-init.log      # partial output from deployment operations?  
# cfn-wire.log      # junk, sqs stuff


#Watch Logs Live
alias logs='log'
alias log='sudo tail -f /var/log/httpd/access_log /var/log/httpd/error_log /var/log/messages /var/log/web.stdout.log /var/log/cfn-init-cmd.log /var/log/cfn-init.log /var/log/eb-engine.log /var/log/eb-cfn-init.log /var/log/eb-publish.log'
alias logw='sudo tail -f /var/log/httpd/access_log /var/log/httpd/error_log /var/log/web.stdout.log'  # mostly the web stuff

alias logaws="logeb"
alias logdeploy="logeb"
alias logeb="sudo tail -f /var/log/cfn-init-cmd.log /var/log/cfn-init.log /var/log/eb-engine.log /var/log/eb-cfn-init.log /var/log/eb-publish.log"

alias logh='loghttpd'
alias loga='loghttpd'
alias logapache='loghttpd'
alias loghttpd='sudo tail -f /var/log/httpd/*'

alias logd='logdjango'
alias logu='logdjango'
alias logdjango='sudo tail -f  /var/log/web.stdout.log'  # I guess it's sudo to protect it?

alias logm='loggunicorn'
alias logmessages='sudo tail -f /var/log/messages'

# Bash Utility
alias sudo="sudo " # allows the use of all our aliases with the sudo command
alias watch="watch "
alias n='nano -c' # nano with syntax highlighting
alias no="nano -Iwn" # nano in overwrite mode to allow for quick pasted overwrites
alias sn='sudo nano -c'
alias sno="sudo nano -Iwn"
alias ls='ls --color=auto -h'
alias l='ls'
alias la='ls -A'
alias lla='ll -A'
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

#Developer tools
alias db='cd /var/app/current; python /var/app/current/manage.py shell_plus'
alias ipy="ipython"
alias manage="python manage.py"
alias shell="python manage.py shell_plus"
alias showmigrations='manage showmigrations'
alias ag="clear; printf '_%.0s' {1..100}; echo ''; echo 'Silver results begin here:'; ag --column"
alias pyc='find . -type f -name "*.pyc" -delete -print'

#htop!
alias htop="htop -d 5"
