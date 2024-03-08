import click
from elasticsearch import Elasticsearch
import json
import csv
import numpy as np
from scipy.stats import pearsonr, spearmanr, kendalltau
from sklearn.metrics import cohen_kappa_score, accuracy_score, confusion_matrix
from pathlib import Path
import re

fields_pages = ["img_fwr", "img_ttr", "img_num_words", "a_fwr", "a_ttr", "a_num_words",
                "meta_fwr", "meta_ttr", "meta_num_words", "h1_fwr", "h1_ttr", "h1_num_words",
                "title_len", "num_h1", "num_h2", "ph_ratio", "num_img", "links", "num_ldjson", "anchor_len_ratio",
                "anchor_words_ratio", "num_words", "fwr", "ttr", "flesch_ease", "anchor_len", "anchor_words", "flesch_grade",
                "url_path_depth", "url_path_len", "url_path_digits", "url_hyphen_path_ratio",  "self_links", "self_link_ratio", "nofollow_links", "num_chars", "affiliate_links", "src_file"
                ]
fields_sites = ["avg_url_path_len", "median_url_path_len", "stddev_url_path_len", "avg_anchor_words",
                    "median_anchor_words", "stddev_anchor_words", "avg_a_num_chars", "median_a_num_chars",
                    "stddev_a_num_chars", "avg_a_ttr", "median_a_ttr", "stddev_a_ttr", "avg_src_offset",
                    "median_src_offset", "stddev_src_offset", "avg_meta_num_chars", "median_meta_num_chars",
                    "stddev_meta_num_chars", "avg_a_num_words", "median_a_num_words", "stddev_a_num_words",
                    "avg_img_num_chars", "median_img_num_chars", "stddev_img_num_chars", "avg_img_ttr",
                    "median_img_ttr", "stddev_img_ttr", "avg_h1_fwr", "median_h1_fwr", "stddev_h1_fwr",
                    "avg_affiliate_product_links", "median_affiliate_product_links", "stddev_affiliate_product_links",
                    "avg_nofollow_links", "median_nofollow_links", "stddev_nofollow_links", "avg_affiliate_links",
                    "median_affiliate_links", "stddev_affiliate_links", "avg_url_hyphen_path_ratio",
                    "median_url_hyphen_path_ratio", "stddev_url_hyphen_path_ratio", "avg_ttr", "median_ttr",
                    "stddev_ttr", "maj_is_review", "avg_flesch_grade", "median_flesch_grade", "stddev_flesch_grade",
                    "avg_self_link_ratio", "median_self_link_ratio", "stddev_self_link_ratio", "avg_fwr", "median_fwr",
                    "stddev_fwr", "avg_meta_num_words", "median_meta_num_words", "stddev_meta_num_words",
                    "avg_flesch_ease", "median_flesch_ease", "stddev_flesch_ease", "avg_num_ldjson",
                    "median_num_ldjson", "stddev_num_ldjson", "avg_anchor_len_ratio", "median_anchor_len_ratio",
                    "stddev_anchor_len_ratio", "avg_img_num_words", "median_img_num_words", "stddev_img_num_words",
                    "avg_num_h2", "median_num_h2", "stddev_num_h2", "avg_affiliate_nofollow_links",
                    "median_affiliate_nofollow_links", "stddev_affiliate_nofollow_links", "maj_has_og",
                    "avg_url_path_depth", "median_url_path_depth", "stddev_url_path_depth", "avg_a_fwr",
                    "median_a_fwr", "stddev_a_fwr", "maj_has_bread", "avg_title_len", "median_title_len",
                    "stddev_title_len", "avg_url_path_digits", "median_url_path_digits", "stddev_url_path_digits",
                    "avg_h1_num_words", "median_h1_num_words", "stddev_h1_num_words", "avg_anchor_len",
                    "median_anchor_len", "stddev_anchor_len", "avg_self_links", "median_self_links",
                    "stddev_self_links", "avg_meta_fwr", "median_meta_fwr", "stddev_meta_fwr", "avg_h1_num_chars",
                    "median_h1_num_chars", "stddev_h1_num_chars", "avg_num_chars", "median_num_chars",
                    "stddev_num_chars", "avg_num_words", "median_num_words", "stddev_num_words", "avg_links",
                    "median_links", "stddev_links", "avg_num_h1", "median_num_h1", "stddev_num_h1", "avg_num_img",
                    "median_num_img", "stddev_num_img", "avg_anchor_words_ratio", "median_anchor_words_ratio",
                    "stddev_anchor_words_ratio", "avg_h1_ttr", "median_h1_ttr", "stddev_h1_ttr", "avg_meta_ttr",
                    "median_meta_ttr", "stddev_meta_ttr", "avg_ph_ratio", "median_ph_ratio", "stddev_ph_ratio",
                "num_docs"]

RE_HIT_NUMBER = re.compile(r'hit-\d+')


def correlations_search(es, link_lt, link_gt, indexp, pagination_size=10000,
                        sites_or_pages='sites', output_file_name='results-pages-2plus.json'):
    pages_q = {
        "bool": {
            "filter": [{
                "range": {
                    "affiliate_links": {
                        "lt": link_lt,
                        "gte": link_gt
                    }}},
                {
                    "range": {
                        "date": {
                            "format": "strict_date_optional_time",
                            "gte": "1970-01-01T08:14:47.631Z",
                            "lte": "2023-02-20T08:14:47.631Z"
                        }
                    }
                },
                {
                    "regexp": {
                        "src_file.keyword": ".+:query-[0-9]+/hit-[0-9]/.+"
                    }
                }
            ]
        }}
    sites_q = {
        "bool": {
        "must": [],
        "filter": [
            {
            "range": {
                "median_affiliate_links": {
                "lt": 100,
                "gte": None
                }
            }
            },
            {
            "range": {
                "num_docs": {
                "lt": 300,
                "gte": None
                }
            }
            }
        ],
        "should": [],
        "must_not": []
        }
    }

    page = 0
    last_size = pagination_size

    def _make_call():
        try:
            if sites_or_pages == 'pages':
                return es.search(index=indexp, query=pages_q, size=pagination_size, fields=fields_pages,
                                 script_fields={}, stored_fields=["*"], search_after=[page],
                                 sort={"_doc": "asc"})
            if sites_or_pages == 'sites':
                return es.search(index=indexp, query=sites_q, size=pagination_size, fields=fields_sites, script_fields={},
                                 stored_fields=["*"], search_after=[page], sort={"_doc": "asc"})
        except Exception as e:
            return _make_call()

    with open(output_file_name, 'w') as of:
        while last_size == pagination_size:
            res = _make_call()
            last_size = len(res['hits']['hits'])
            print(last_size)
            page = res['hits']['hits'][-1]['sort'][0]
            of.writelines([f"{json.dumps(line)}\n" for line in res['hits']['hits']])


def _json_to_cvs(se_stats_json_file, output_file, fields=None, skip='warc_date,src_file'):
    def _getter(dic, name):
        try:
            return str(dic[name][0])
        except Exception as e:
            return '0'
    skip = skip.split(',')

    of = open(output_file, 'w')
    of.write(",".join(fields) + '\n')
    for line in open(se_stats_json_file):
        f = json.loads(line)['fields']
        of.write(",".join([_getter(f, field) for field in fields]) + '\n')
    of.close()


def _correlation_from_es_results(se_stats_cvs_file, key_field='affiliate_links', min_key=0, max_key=100, statistic='pearson'):
    def _to_float(x):
        try:
            return float(x)
        except ValueError:
            return x

    results = open(se_stats_cvs_file, 'r').readlines()
    header = [x.strip() for x in results[0].split(",")]
    key_index = header.index(key_field)
    aff_index = header.index('affiliate_links')

    results = np.asarray([[_to_float(x.strip())
                           for x in line.split(",")]
                          for line in results[1:] if min_key <= float(line.split(",")[aff_index].strip()) <= max_key])
    y_row = results[:, key_index]
    print(f'examples {min_key}--{max_key}: ', len(y_row))
    if key_field == 'src_file':
        y_row = np.asarray([float(RE_HIT_NUMBER.search(y).group(0).strip('hit-')) for y in y_row])

    permuted_results = np.moveaxis(results, 1, 0)

    if statistic == 'pearson':
        return zip(header, [pearsonr(x_row, y_row).statistic for x_row in permuted_results])
    elif statistic == 'spearman':
        return zip(header, [spearmanr(x_row, y_row).correlation for x_row in permuted_results])
    elif statistic == 'kendall':
        return zip(header, [kendalltau(x_row, y_row, variant='c').correlation for x_row in permuted_results])

    raise KeyError("statistic must be either `pearson` `spearman` or `kendall` ")


def print_correlation_results(correlations: list):
    def _highlight(score):
        if score >= 0.3 or score <= -0.3:
            s = "{\\bf " + str("%.2f" % score) + "}"
        elif 0.1 <= score < 0.3 or -0.1 >= score > -0.3:
            s = str("%.2f" % score)
        else:
            s = "\\no{" + str("%.2f" % score) + "}"

        if score >= 0.1:
            s = "\\gre{" + s + "}"
        elif score <= -0.1:
            s = "\\red{" + s + "}"

        return s

    results = {}
    for ind, p in enumerate(correlations):
        for field, tup in p:
            results.setdefault(field, [0] * len(correlations))[ind] = round(tup, 2)

    with open('scratch.txt', 'w') as of:
        for field in fields_pages:
            of.write(f"{field} \t\t\t\t & {' & '.join([_highlight(elem) for elem in results[field]]) } \\\\ \n")


def get_statistics(esurl, apik, key, indexp):
    # download sites statistics
    es = Elasticsearch(hosts=[f"https://{esurl}:9200"], api_key=apik)
    correlations_search(es, 100, 1, indexp=indexp, sites_or_pages='pages',
                        output_file_name=f'affiliate-stats-{key}.jsonl')
    _json_to_cvs(f'affiliate-stats-{key}.jsonl', f'affiliate-stats-{key}.cvs', fields=fields_pages)
    # _json_to_cvs('results-pages-2plus.json', f'affiliate-stats-{key}.cvs', fields=fields_pages)
    # _json_to_cvs('results-sites-2plus.json', 'affiliate-stats-cc-sites.cvs')


def link_correlation_table(input_files: list, statistic='spearman'):
    r = []
    for i in input_files:
        print(i)
        r.append(_correlation_from_es_results(i, min_key=0, max_key=10, statistic=statistic))
        r.append(_correlation_from_es_results(i, min_key=11, max_key=35, statistic=statistic))
        r.append(_correlation_from_es_results(i, min_key=36, max_key=100, statistic=statistic))

    print_correlation_results(r)


def rank_correlation_table(input_files: list, statistic='spearman'):
    r = []
    for i in input_files:
        if 'src_file' not in open(i, 'r').readline().strip().split(','):
            print(f"skip {i}")
            continue
        print(i)
        r.append(_correlation_from_es_results(i, min_key=0, max_key=10, key_field='src_file', statistic=statistic))
        r.append(_correlation_from_es_results(i, min_key=11, max_key=35, key_field='src_file', statistic=statistic))
        r.append(_correlation_from_es_results(i, min_key=36, max_key=100, key_field='src_file', statistic=statistic))

    print_correlation_results(r)


def site_classification_kappa(annotations_1: str, annotations_2: str) -> None:
    with open(annotations_1) as inf:
        reader = csv.reader(inf, delimiter=',', quotechar='"')
        anno_1 = list(reader)
        votes_1 = [line[1] for line in anno_1]
    with open(annotations_2) as inf:
        reader = csv.reader(inf, delimiter=',', quotechar='"')
        anno_2 = list(reader)
        votes_2 = [line[1] for line in anno_2]

    # differences
    for a, b in zip(anno_1, anno_2):
        if a[1] != b[1]:
            print(a[0], a[1], b[1])

    print("cohen kappa", cohen_kappa_score(votes_1, votes_2))
    print("accuracy", accuracy_score(votes_1, votes_2))
    print(confusion_matrix(votes_1, votes_2))

@click.group()
def main():
    pass


@click.option('-e', '--esurl', type=str, default="elasticsearch.bw.webis.de", help='the web address of the cluster.')
@click.option('-a', '--api_key', type=str, help='api key to access elastic.')
@click.option('-k', '--key', type=str, help='a key to identify the output.')
@click.option('-i', '--indexp', type=str, help='The index pattern.')
@main.command()
def run(esurl: str, api_key: str, key: str, indexp: str):
    """ Get the count data from elastic search and do the statistics """
    get_statistics(esurl, api_key, key, indexp)


@click.option('-i', '--input_dir', type=click.Path(), help='Input dir with csv files.', default='./csv', show_default=True)
@click.option('-t', '--table', type=int, help='Which table. 1: link correlation; 2: rank correlation', default='./csv', show_default=True)
@main.command()
def build_tables(input_dir, table):
    if table == 1:
        link_correlation_table(sorted(list(Path(input_dir).glob("*.cvs"))))
    elif table == 2:
        rank_correlation_table(sorted(list(Path(input_dir).glob("*.cvs"))))


if __name__ == "__main__":
    main()
