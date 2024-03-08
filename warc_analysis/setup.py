import setuptools

# setup.py for installing Beam pipeline dependencies
setuptools.setup(
    name='affiliate-warc-analyzer',
    install_requires=[
        'apache-beam[aws,dataframe]',
        'click',
        'elasticsearch<8.0.0',
        'publicsuffix',
        'requests',
        'resiliparse[all]',
        'textstat'
    ],
    packages=['warc_analysis']
)
