# Is Google Getting Worse? A Longitudinal Investigation of SEO Spam in Search Engines

Code and data for the ECIR'24 paper "Bevendorff et al., Is Google Getting Worse? A Longitudinal Investigation of SEO Spam in Search Engines".

## Usage

The code has the following prerequisites:

- Python 3.10+
- An Apache Beam cluster (for crawling and WARC analysis)
- An Elasticsearch cluster (for WARC analysis)

```bash
pip install poetry
poetry install
pip install apache-beam   # Needs to be done manually
poetry shell              # Opens a new shell inside the created venv
```
Next, create the local config files `serp_crawler/serp_crawler/local_config.py` `warc_analysis/warc_analysis/local_config.py`. These should include the required configuration for accessing your Beam and Elasticsearch clusters. See the `config.py` file in the same directory for reference. Not all tools require a valid Beam / Elasticsearch config, but at least the `local_config.py` file needs to exist.

Inside the Poetry shell, the following commands are available:

```bash
serp-crawl        # Crawling and scraping tools
warc-analyze      # Result WARC analysis
```

Use ``--help`` for usage instructions.


## Webis Product SERP Corpus 2024

The product SERP dataset can be downloaded from [Zenodo](https://doi.org/10.5281/zenodo.10797507) (research purposes only, no redistribution).

The list of product queries can be found in [serp_crawler/resources/product-queries.txt](serp_crawler/resources/product-queries.txt).

The full website crawls are too large to publish on Zenodo and can be made available on requests (please contact us).

## How to cite

```bibtex
@InProceedings{bevendorff:2024a,
  author =                   {Janek Bevendorff and Matti Wiegmann and Martin Potthast and Benno Stein},
  booktitle =                {Advances in Information Retrieval. 46th European Conference on IR Research (ECIR 2024)},
  doi =                      {10.1007/978-3-031-56063-7_4},
  month =                    mar,
  publisher =                {Springer},
  series =                   {Lecture Notes in Computer Science},
  site =                     {Glasgow, Scotland},
  title =                    {{Is Google Getting Worse? A Longitudinal Investigation of SEO Spam in Search Engines}},
  year =                     2024
}
```
