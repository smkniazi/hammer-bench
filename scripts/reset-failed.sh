#!/bin/bash

MDS_NODES=(`grep -v "^#" mds-nodes`)
pssh -H "${MDS_NODES[*]}"  -l ubuntu -i  sudo systemctl reset-failed
