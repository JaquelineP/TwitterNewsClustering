#!/bin/bash
##########################
# Install Git, ZSH, tmux
#########################
sudo yum install git tmux zsh 

################
# configure tmux
#################
touch ~/.tmux.conf
echo "setw -g mode-mouse on" >> ~/.tmux.conf
echo "set -g mouse-select-pane on" >> ~/.tmux.conf
echo "set -g mouse-select-window on" >> ~/.tmux.conf
echo "set-option -g history-limit 3000" >> ~/.tmux.conf

##################
# install oh-my-zsh
###################
sh -c "$(wget https://raw.githubusercontent.com/robbyrussell/oh-my-zsh/master/tools/install.sh -O -)"

###################
# Copy data to HDFS
##################
hadoop fs -put twitter.dat hdfs:///twitter.dat

