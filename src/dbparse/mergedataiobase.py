""" Base IO Class For Merging Data """

__all__ = ["MergeDataIOBase"]

from dbbase import MusicDBDataIO, MusicDBIDModVal
from dbbase import MusicDBRootDataIO
from .modvaldataoutput import ModValDataOutput


class MergeDataIOBase:
    def __repr__(self):
        return f"MergeDataIOBase(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.dataio = MusicDBDataIO(rdio, **kwargs)
        self.mvdopt = ModValDataOutput(rdio, **kwargs)
        self.db = rdio.db
        self.mv = MusicDBIDModVal()
        self.procs = {}

    #############################################################################################
    # ModVals and When To Show
    #############################################################################################
    def isUpdateModVal(self, n):
        if self.verbose is False:
            return False
        retval = True if ((n + 1) % 25 == 0 or (n + 1) == 5) else False
        return retval
    
    #############################################################################################
    # Pretty Print Stats
    #############################################################################################
    def showStats(self, stats):
        retval = "     ".join([f"{key} = {val}" for key, val in stats.items()]) if isinstance(stats, dict) else ""
        return retval
    
    #############################################################################################
    # Merge Runner
    #############################################################################################
    def merge(self, modVal=None, key=None, **kwargs):
        if key in self.procs.keys():
            self.procs[key].merge(modVal=modVal, **kwargs)
        else:
            for key, proc in self.procs.items():
                proc.merge(modVal=modVal, **kwargs)