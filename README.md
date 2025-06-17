This repo is part of the [SOMISANA](https://somisana.ac.za/) initiative, and used for downloading data from various sources in support of our operational models as well as for running hindcasts.

## Installation instructions 

Start off by cloning this repo to your local machine (do this wherever you would like the code):

```sh
git clone git@github.com:SAEON/somisana-download.git
```

Alternatively, if you're not anticipating developing and pushing changes to this repo, you can also use HTTPS protocol (only do one of these clone commands!):

```sh
git clone https://github.com/SAEON/somisana-download.git
```

Then navigate to the root directory of the repo:
`cd somisana-download`

Then create a new conda environment called `download`, with all the required dependencies read from the `environment.yml` file:
```sh
mamba env create -f environment.yml
```
or use `conda` instead of `mamba` if you haven't moved to mamba yet

Then activate the environment
```sh
conda activate download
```

and install the python library so it's available in your environment:
```sh
pip install --no-deps -e .
```

If it's been a while since you first created the environment, you may want to update it so that your local environment uses the latest versions of the packages (especially copernicusmarine, which gets updated often as not always backwards compatible). You can do this by:

```sh
mamba env update -f environment.yml --prune
```
