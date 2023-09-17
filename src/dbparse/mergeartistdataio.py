""" Base Class For Merging Artist ModVal Data """

__all__ = ["MergeArtistDataIO"]

from dbbase import MusicDBRootDataIO
from utils import Timestat
from pandas import concat, Series
from numpy import array_split, ndarray, int64
from .mergedataiobase import MergeDataIOBase
from .mergefiletype import MergeFileType


class MergeArtistDataIO(MergeDataIOBase):
    def __repr__(self):
        return f"MergeArtistDataIO(db={self.dataio.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, mergeType: MergeFileType, **kwargs):
        super().__init__(rdio, **kwargs)
        self.mergeType = mergeType
        assert isinstance(mergeType.inputName, list), f"MergeArtistDataIO.inputName must be a list of inputNames"
        
        if self.verbose:
            outputName = mergeType.outputName if isinstance(mergeType.outputName,str) else "ModVal"
            print(f"  {'MergeArtistDataIO:': <25} [{mergeType.inputName}] => [{outputName}]")
        
        
    #############################################################################################
    # Merge Sub Artist Data
    #############################################################################################
    def merge(self, modVal=None, force=False, **kwargs):
        mergeArgs       = kwargs
        modVals         = self.getModVals(modVal)
        if self.verbose: ts = Timestat(f"Merging {len(modVals)} Artist ModVal Data")

        for n,modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                if self.verbose: ts.update(n=n+1,N=len(modVals))

            mergeStats = {"Start": {}, "Merged": {}, "Missing": {}, "End": 0}
            inputNames = self.mergeType.inputName
            modValData = self.dataio.getFileTypeModValData(modVal, inputNames[0], force=False)
            assert isinstance(modValData,Series), f"MergeArtistDataIO expected an Artist Series object, but found a [{type(modValData)}]"
            mergeStats["Start"][inputNames[0]] = len(modValData)
            
            for inputName in inputNames[1:]:
                inputModValData = self.dataio.getFileTypeModValData(modVal, inputName, force=False)
                mergeStats["Start"][inputName] = len(modValData)
                mergeStats["Merged"][inputName]  = 0
                mergeStats["Missing"][inputName] = 0
                if not isinstance(inputModValData,Series) or len(inputModValData) == 0:
                    continue
                for artistID,artistIDData in inputModValData.iteritems():
                    if modValData.get(artistID) is None:
                        mergeStats["Missing"][inputName] += 1
                        continue
                    modValData[artistID].merge(artistIDData, **mergeArgs)
                    mergeStats["Merged"][inputName] += 1
                        
            mergeStats["End"] = len(modValData)
            return self.mvdopt.save(modValData, modVal, self.mergeType, mergeStats)
            
        if self.verbose: ts.stop()