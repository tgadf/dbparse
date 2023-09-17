""" Base Class For Merging Shuffle ModVal Data """

__all__ = ["MergeShuffleDataIO"]

from dbbase import MusicDBRootDataIO
from utils import Timestat
from pandas import concat, Series
from numpy import array_split, ndarray, int64
from .mergedataiobase import MergeDataIOBase
from .mergefiletype import MergeFileType


class MergeShuffleDataIO(MergeDataIOBase):
    def __repr__(self):
        return f"MergeShuffleDataIO(db={self.dataio.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, mergeType, **kwargs):
        super().__init__(rdio, **kwargs)
        self.mergeType = mergeType
        
        if self.verbose:
            print(f"  {'MergeShuffleDataIO:': <25} [{mergeType.inputName}] => [{mergeType.outputName}]")
                
    #################################################################################################
    # Merge Sub Artist Data
    #################################################################################################
    def merge(self, modVal=None, force=False):
        modVals        = self.getModVals(modVal)
        if self.verbose: ts = Timestat(f"Merging {len(modVals)} Shuffle ModVal Data")

        for n,modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                if self.verbose: ts.update(n=n+1,N=len(modVals))

            mergeStats = {"Start": 0, "Files": 0, "Merged": 0, "New": 0, "End": 0}
            files      = self.dataio.getNewShuffleArtistFiles(modVal, self.mergeType.inputName, tsFileType=self.mergeType.outputName, force=force, verbose=False)
            modValData = self.dataio.getFileTypeModValData(modVal, self.mergeType.outputName, force=True)
            mergeStats["Start"] = len(modValData)
            for ifile in files:
                mergeStats["Files"] += 1
                inputModValData = self.io.get(ifile)
                assert isinstance(inputModValData,Series), f"File data is not a Series [{type(inputModValData)}] from [{ifile}]"
                for artistID,artistIDData in inputModValData.iteritems():
                    if modValData.get(artistID) is None:
                        modValData[artistID] = artistIDData
                        mergeStats["New"] += 1
                    else:
                        modValData[artistID].merge(artistIDData)
                        mergeStats["Merged"] += 1
                    if modValData.get(artistID) is None:
                        mergeStats["Missing"] += 1
                        continue
                        
            modValData = Series(modValData)
            mergeStats["End"] = len(modValData)
            if mergeStats["Files"] > 0:
                return self.mvdopt.save(modValData, modVal, self.mergeType, mergeStats)
            else:
                if self.verbose: print(f"   ===> Not Saving {self.mergeType.outputName} ModVal={modVal} Data.")
            
        if self.verbose: ts.stop()