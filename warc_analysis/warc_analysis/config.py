import os

_CONFIG = None

CONFIG = dict(elasticsearch=dict(
        hosts=[],
        use_ssl=True,
        api_key=[],
        timeout=240,
        retry_on_timeout=True
    ),
    # Apache Beam pipeline options
    pipeline_opts=dict(
        runner='PortableRunner',
        setup_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'setup.py'),
        flink_master='localhost:8081',
        environment_type='LOOPBACK',
        s3_endpoint_url='your S3 endpoint',
        s3_access_key_id='your access key',
        s3_secret_access_key='your secret key',
    ),
)


def get_config():
    """
    Load application configuration.
    """
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = CONFIG.copy()
        try:
            import warc.local_config
            _CONFIG.update(warc.local_config.CONFIG)
        except ImportError:
            raise RuntimeError("Could not find local_config.py.")

    return _CONFIG
