#!/bin/bash

# script that setups a tmux session with three panes that attach to the log files
# of the node and the two workers launched by `./launch.py`

#################################################################################
# If you work with docker:
#
# 1.  run: ./launch.py in docker
# 2.  open a new bash session in a new window in the running container:
#     docker exec -it [container-id] bash
# 3.  run this script: ./tmux_logger.sh
#################################################################################


if tmux has-session -t substratee_logger ; then
  echo "detected existing polkadot logger session, attaching..."
else
  # or start it up freshly
  tmux new-session -d -s substratee_logger \; \
    split-window -v \; \
    split-window -v \; \
    select-layout even-vertical \; \
    send-keys -t substratee_logger:0.0 'tail -f ../log/node.log' C-m \; \
    send-keys -t substratee_logger:0.1 'tail -f ../log/worker1.log' C-m \; \
    send-keys -t substratee_logger:0.2 'tail -f ../log/worker2.log' C-m

    # Attention: Depending on your tmux conf, indexes may start at 1

    tmux setw -g mouse on
fi
tmux attach-session -d -t substratee_logger