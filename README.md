# Inscrits (SISE) v2

## Project Overview

This project involves managing and processing data related to registrations (Inscrits) using the SISE system. The main script `MAIN.py` handles the overall program flow, including data initialization and processing.


## Table of Contents

- [Setup Instructions](#setup-instructions)
  - [Configuration](#configuration)
  - [Directory Structure](#directory-structure)
- [Main Script](#main-script)
  - [DATA_INIT](#step-1-data_init)
    - [SISE_READ](#sise_read)


## Setup

### Configuration

1. **Create a root file `config_path.py` with the path to your project folder. This file is ignored by Git.**
    PATH: This variable should point to the general 'inscrits' folder outside of the GitHub code repository.


    PATH = '/path/to/your/inscrits/folder'

    Ensure the 'inscrits' folder contains the following subdirectories:

      - input/: Directory for input data files
      - output/: Directory for output data files



## Main Script
MAIN.py
This script is responsible for the overall management of the program. Below is an outline of its functionality:

### Step 1: DATA_INIT
This step should be executed at the launch of the campaign with INITIALISATION set to TRUE.
  #### NOMENCLATURES
  - load and update BCN, LES_COMMUNES, PAYSAGE_id
  
  #### SISE_READ
  - Export Data Review: data_review_(year).xlsx to check the data.
  - items_by_vars: Lists all items per variable and their frequency by source.

  - output
      - zip sise_parquet: save per year a complete SISE file with selected columns (as declared in utils).