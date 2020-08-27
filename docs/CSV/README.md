# rename.sh

A renaming script for data from the simulation.

## Prerequisites

`bash` and `sed`.

## Usage

This script will take the output files of a run of the simulation and rename them according to the assumptions of
the browser visualization. This is necessary, as the visualization does not include any server logic to allow
detection of files without knowing their name.

Simply dump all CSV files into this folder and run `./rename.sh`.
