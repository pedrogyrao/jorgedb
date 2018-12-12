from pkg_resources import resource_filename

from request_chain import __name__ as PACKAGE_NAME
DEFAULT_PATH = resource_filename(PACKAGE_NAME, '/jorge')
