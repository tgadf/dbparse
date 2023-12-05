""" Base IO Class For Concating Data """

__all__ = ["ConcatDataIOBase"]

from dbbase import MusicDBDataIO, MusicDBIDModVal
from dbbase import MusicDBRootDataIO
from .modvaldataoutput import ModValDataOutput
from .groupdatadirio import ShuffleDataDirIO


class ConcatDataIOBase:
    def __repr__(self):
        return f"ConcatDataIOBase(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.dataio = MusicDBDataIO(rdio, **kwargs)
        self.sddio = ShuffleDataDirIO(rdio, **kwargs)
        self.mvdopt = ModValDataOutput(rdio, **kwargs)
        self.db = rdio.db
        self.mv = MusicDBIDModVal()
        self.procs = {}

    ###########################################################################
    # ModVals and When To Show
    ###########################################################################
    def isUpdateModVal(self, n):
        if self.verbose is False:
            return False
        retval = True if ((n + 1) % 25 == 0 or (n + 1) == 5) else False
        return retval
    
    ###########################################################################
    # Pretty Print Stats
    ###########################################################################
    def showStats(self, stats):
        retval = "     ".join([f"{key} = {val}" for key, val in stats.items()]) if isinstance(stats, dict) else ""
        return retval
    
    ###########################################################################
    # Concat Runner
    ###########################################################################
    def concat(self, modVal=None, key=None, **kwargs):
        if isinstance(key, str):
            assert key in self.procs.keys(), f"Invalid key [{key}]: {self.procs.keys()}"
        if key in self.procs.keys():
            self.procs[key].concat(modVal=modVal, **kwargs)
        else:
            for key, proc in self.procs.items():
                proc.concat(modVal=modVal, **kwargs)
                
    ###########################################################################
    # Helper Base Functions
    ###########################################################################
    def getShuffleFiles(self, modVal, concatType, verbose=False, **kwargs) -> 'list':
        newFiles = self.sddio.getNewFiles(modVal=modVal, fileType=concatType.inputName,
                                          tsFile=None, force=True, verbose=verbose)
        return newFiles