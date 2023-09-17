""" Base IO Class For Groupby Data """

__all__ = ["GroupDataIOBase"]

from dbmaster import MasterBasic
from dbbase import RawDataIOBase, MusicDBDataIO, MusicDBIDModVal, getModVals
from utils import FileIO, Timestat
from pandas import Series


class GroupDataIOBase:
    def __init__(self, mdbdata, mdbdir, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.dataio  = MusicDBDataIO(mdbdata, mdbdir, **kwargs)
        self.rdbio   = RawDataIOBase()
        self.db      = mdbdir.db
        self.io      = FileIO()
        self.mv      = MusicDBIDModVal()
        self.procs   = {}
        self.getModVals = getModVals


    ##################################################################################################
    # ModVals and When To Show
    ##################################################################################################    
    def isUpdateModVal(self, n):
        if self.verbose is False:
            return False
        retval = True if ((n+1) % 25 == 0 or (n+1) == 5) else False
        return retval
    
    
    ##################################################################################################
    # Pretty Print Stats
    ##################################################################################################
    def showStats(self, stats):
        retval = "     ".join([f"{key} = {val}" for key,val in stats.items()]) if isinstance(stats,dict) else ""
        return retval
    
    
    ##################################################################################################
    # Group Runner
    ##################################################################################################
    def group(self, modVal=None, key=None, **kwargs):
        if key in self.procs.keys():
            self.procs[key].group(modVal=modVal, **kwargs)
        else:
            for key,proc in self.procs.items():
                proc.group(modVal=modVal, **kwargs)