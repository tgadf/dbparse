""" Base Class For Merging/Concating Artist ModVal Data """

__all__ = ["MergeArtistConcatDataIO"]

from dbbase import MergeFileType, RawDataIOBase
from utils import Timestat
from pandas import concat, Series
from numpy import array_split, ndarray, int64
from .mergedataiobase import MergeDataIOBase


class MergeArtistConcatDataIO(MergeDataIOBase):
    def __init__(self, mdbdata, mdbdir, mergeType, **kwargs):
        super().__init__(mdbdata, mdbdir, **kwargs)
        self.mergeType = mergeType
        self.rdbio = RawDataIOBase()
        assert isinstance(mergeType.inputName,list), f"MergeArtistConcatDataIO.inputName must be a list of inputNames"
        
        if self.verbose:
            outputName = mergeType.outputName if isinstance(mergeType.outputName,str) else "ModVal"
            print(f"  {'MergeArtistConcatDataIO:': <25} [{mergeType.inputName}] => [{outputName}]")
        
        
    ###########################################################################################
    # Merge Sub Artist Data
    ###########################################################################################
    def merge(self, modVal=None, force=False, **kwargs):
        mergeArgs       = kwargs
        modVals         = self.getModVals(modVal)
        if self.verbose: ts = Timestat(f"Merging {len(modVals)} Artist ModVal Data")

        for n,modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                if self.verbose: ts.update(n=n+1,N=len(modVals))

            mergeStats = {"Start": {}, "Merged": {}, "Concat": {}, "End": 0}
            inputNames = self.mergeType.inputName
            modValData = self.dataio.getFileTypeModValData(modVal, inputNames[0], force=False)
            assert isinstance(modValData,Series), f"MergeArtistDataIO expected an Artist Series object, but found a [{type(modValData)}]"
            mergeStats["Start"][inputNames[0]] = len(modValData)
            
            for inputName in inputNames[1:]:
                inputModValData = self.dataio.getFileTypeModValData(modVal, inputName, force=False)                
                mergeStats["Start"][inputName]  = len(inputModValData)
                mergeStats["Merged"][inputName] = 0
                mergeStats["Concat"][inputName] = 0
                if not isinstance(inputModValData,Series) or len(inputModValData) == 0:
                    continue
                    
                for artistID,artistIDData in inputModValData[inputModValData.index.isin(modValData.index)].iteritems():
                    modValData[artistID].merge(artistIDData, **mergeArgs)
                    mergeStats["Merged"][inputName] += 1
                    
                newData = inputModValData[~inputModValData.index.isin(modValData.index)]
                mergeStats["Concat"][inputName] = len(newData)
                modValData = concat([modValData, newData])
                    
                        
            mergeStats["End"] = len(modValData)
            return self.mvdopt.save(modValData, modVal, self.mergeType, mergeStats)
            
        if self.verbose: ts.stop()