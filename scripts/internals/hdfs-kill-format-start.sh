#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

        source ./internals/kill-processes-on-all-machines.sh .*java        
	
        source ./internals/formatNN.sh

        source ./internals/start-NNs.sh

        source ./internals/start-DNs.sh

