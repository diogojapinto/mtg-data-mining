# MTG Data Mining

A repository dedicated to mining MTG data. Check the [project's page](https://github.com/users/diogojapinto/projects/1/views/1) for the status of it.

## Maniging the Conda environment

This repository uses `conda` to manage the development environment. The quickest way to get started is through installing [miniconda](https://docs.conda.io/en/latest/miniconda.html).

_[Here's a cheat sheet on conda](https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf)._

**Note:** The environment provided in this repo is compatible with Windows 11 OS - 64 bits.

### Loading the environment

If you haven't set up the environment yet, configure it based on the environment file and load it like so:

```bash
conda env create -f environment.yml
conda activate mtg-data-mining
```

If you already had the environment, and just want to update it:

```bash
conda activate mtg-data-mining
conda env update --file environment.yml --prune
```

### Exporting the environment

As one modifies the environment's packages, the environment file should be updated as follows:

```bash
conda env export | grep -v "^prefix: " > environment.yml
```

## Credits

- [17Lands](https://www.17lands.com/)
  - [API documentation](https://www.17lands.com/ui/)
  - Support them on [Patreon](https://www.patreon.com/17lands)
- [Scryfall](https://scryfall.com/)
  - [API documentation](https://scryfall.com/docs/api)
  - Support them through [donations](https://scryfall.com/donate)