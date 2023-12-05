""" Base IO Class For Merging Data """

__all__ = ["MergeDataIOBase"]

from dbbase import MusicDBDataIO, MusicDBIDModVal
from dbbase import MusicDBRootDataIO
from .modvaldataoutput import ModValDataOutput
from .groupdatadirio import ShuffleDataDirIO, ShuffleArtistDataDirIO


class MergeDataIOBase:
    def __repr__(self):
        return f"MergeDataIOBase(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.rdio = rdio
        self.dataio = MusicDBDataIO(rdio, **kwargs)
        self.sddio = ShuffleDataDirIO(rdio, **kwargs)
        self.saddio = ShuffleArtistDataDirIO(rdio, **kwargs)
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
    # Merge Runner
    ###########################################################################
    def merge(self, modVal=None, key=None, **kwargs):
        if isinstance(key, str):
            assert key in self.procs.keys(), f"Invalid key [{key}]: {self.procs.keys()}"
        if key in self.procs.keys():
            self.procs[key].merge(modVal=modVal, **kwargs)
        else:
            for key, proc in self.procs.items():
                proc.merge(modVal=modVal, **kwargs)
                
    ###########################################################################
    # Helper Base Functions
    ###########################################################################
    def getShuffleFiles(self, modVal, mergeType, verbose=False, **kwargs) -> 'list':
        newFiles = self.sddio.getNewFiles(modVal=modVal, fileType=mergeType.inputName,
                                          tsFile=None, force=True, verbose=verbose)
        return newFiles
    
    def getShuffleArtistFiles(self, modVal, mergeType, verbose=False, **kwargs) -> 'list':
        newFiles = self.saddio.getNewFiles(modVal=modVal, fileType=mergeType.inputName,
                                           tsFile=None, force=True, verbose=verbose)
        return newFiles
    