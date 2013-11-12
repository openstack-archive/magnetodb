# vim: tabstop=4 shiftwidth=4 softtabstop=4

from oslo.config import cfg


common_opts = [
    cfg.StrOpt('api_paste_config',
               default="api-paste.ini",
               help='File name for the paste.deploy config for magnetodb-api'),

    cfg.IntOpt('magnetodb_api_workers', default=None),

    cfg.IntOpt('bind_port', default=80)
]

CONF = cfg.CONF
CONF.register_opts(common_opts)


def parse_args(argv, default_config_files=None):
    cfg.CONF(args=argv[1:],
             project='magnetodb',
             default_config_files=default_config_files)
