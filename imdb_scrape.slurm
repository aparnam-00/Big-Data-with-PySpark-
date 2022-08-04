#!/bin/bash
#
#SBATCH --nodes=10
#SBATCH --ntasks=10
#SBATCH --ntasks-per-node=1
#SBATCH --partition=parallel
#SBATCH --time=08:00:00
#SBATCH --account=ds5559

module load anaconda

python imdb_scrape.py
