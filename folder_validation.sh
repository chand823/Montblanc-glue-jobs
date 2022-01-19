#!/bin/sh
if [ ! "$(ls -A ${glue_folder}/src)" ]; then echo "ERROR: src folder is empty!"; exit 1; fi
if [ ! "$(ls -A ${glue_folder}/python)" ]; then echo "ERROR: python folder is empty!"; exit 1; fi
if [ ! "$(ls -A ${glue_folder}/files)" ]; then echo "ERROR: files folder is empty!"; exit 1; fi
if [ ! "$(ls -A ${glue_folder}/jars)" ]; then echo "ERROR: jars folder is empty!"; exit 1; fi
