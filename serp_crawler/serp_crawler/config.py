import os

_CONFIG = None

CONFIG = dict(
    # Apache Beam pipeline options
    pipeline_opts=dict(
        runner='PortableRunner',
        setup_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'setup.py'),
        flink_master='localhost:8081',
        environment_type='LOOPBACK'
    ),
    chatnoir_apikey=None
)


def get_config():
    """
    Load application configuration.
    """
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = CONFIG.copy()
        try:
            import serp_crawler.local_config
            _CONFIG.update(serp_crawler.local_config.CONFIG)
        except ImportError:
            raise RuntimeError("Could not find local_config.py.")

    return _CONFIG
