""" Base Class For Parsing Raw Data """

__all__ = ["ParseRawDataBase"]

from dbbase import MusicDBIDModVal
from dbbase import MusicDBRootDataIO

class ParseRawDataBase:
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.rdio = rdio
        self.mv = MusicDBIDModVal()
        self.db = rdio.db