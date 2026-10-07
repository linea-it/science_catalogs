#!/bin/bash
# Activate the pipeline environment BEFORE submitting this script.
# Example: sbatch examples/configs/dp2/run_to_hats.sbatch /absolute/path/to/config.yml
#SBATCH --job-name=lsst_dp2_to_hats
#SBATCH --partition=cpu_pipelines
#SBATCH --account=hpc-pipelines
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=48G
#SBATCH --time=48:00:00
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

set -euo pipefail
if [ "$#" -ne 1 ]; then
    echo "Usage: sbatch run_to_hats.sbatch /absolute/path/to/config.yml" >&2
    exit 2
fi
science-catalogs "$1"
