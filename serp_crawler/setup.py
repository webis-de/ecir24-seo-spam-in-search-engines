import setuptools

# setup.py for installing Beam pipeline dependencies
setuptools.setup(
    name='affiliate-serp-crawler',
    install_requires=[
        'apache-beam',
        'chatnoir-api',
        'click',
        'httpx',
        'resiliparse',
        'tqdm'
    ],
    packages=['serp_crawler']
)
