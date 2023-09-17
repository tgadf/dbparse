""" Base Class For Merging Media ModVal Data """

__all__ = ["MergeMediaDataIO"]

from dbbase import MergeFileType
from utils import Timestat
from pandas import concat, Series
from numpy import array_split, ndarray, int64
from .mergedataiobase import MergeDataIOBase


class MergeMediaDataIO(MergeDataIOBase):
    def __init__(self, mdbdata, mdbdir, mediaType, **kwargs):
        super().__init__(mdbdata, mdbdir, **kwargs)
        self.mergeType = mediaType
        self.testRequired(mdbdir)
        assert isinstance(self.isRequired,bool), f"Could not determine if MergeMediaData is required"
        
        if self.verbose:
            print(f"  {'MergeMediaDataIO:': <25} [{self.mergeType.inputName} ⟕ {self.mergeType.mediaName}] => [{self.mergeType.outputName}] [Required = {self.isRequired}]")
        

    ##################################################################################################
    # Test For Need
    ##################################################################################################
    def testRequired(self, mdbdir):
        requireInput  = hasattr(mdbdir, f"getModVal{self.mergeType.inputName}DataDir") and callable(getattr(mdbdir, f"getModVal{self.mergeType.inputName}DataDir"))
        requireOutput = hasattr(mdbdir, f"getModVal{self.mergeType.outputName}DataDir") and callable(getattr(mdbdir, f"getModVal{self.mergeType.outputName}DataDir"))
        self.isRequired = requireInput and requireOutput
        
        
    ##################################################################################################
    # Merge Media Data
    ##################################################################################################
    def merge(self, modVal=None, force=False):
        if self.isRequired is False:
            return
        
        modVals        = self.getModVals(modVal)
        if self.verbose: ts = Timestat(f"Merging {len(modVals)} Media ModVal Data")

        for n,modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                if self.verbose: ts.update(n=n+1,N=len(modVals))

            mergeStats = {"Start": 0, "New": 0, "Matched": 0, "End": 0}
            modValData = self.dataio.getFileTypeModValData(modVal, self.mergeType.outputName, force=force)
            mergeStats["Start"] = len(modValData)
            
            mergedData = None
            files      = self.dataio.getNewFiles(modVal=modVal, fileType=self.mergeType.inputName, tsFileType=self.mergeType.outputName, force=force)
            if len(files) > 0:
                newModValData = concat([Series(self.io.get(ifile)) for ifile in files])
                mergeStats["New"] = len(newModValData)
                modValData = concat([modValData,newModValData]) if (isinstance(modValData,Series) and len(modValData) > 0) else newModValData
                modValData = modValData[~modValData.index.duplicated()]
                
                mediaModValData = self.dataio.getFileTypeModValData(modVal, self.mergeType.media, force=False)
                if isinstance(mediaModValData,Series):
                    matchedData   = mediaModValData[mediaModValData.index.isin(modValData.index)]
                    unmatchedData = modValData[~modValData.index.isin(matchedData.index)]
                    mergedData    = concat([matchedData,unmatchedData])
                    mergeStats["Matched"] = len(matchedData)
                    mergeStats["End"]     = len(mergedData)
                else:
                    mergedData    = modValData
                    mergeStats["Matched"] = 0
                    mergeStats["End"]     = len(mergedData)
            
            
            if any([mergeStats[key] > 0 for key in ["New"]]):
                if self.verbose: print(f"   ===> Saving {self.mergeType.outputName} ModVal={modVal} Data.  [{self.showStats(mergeStats)}]  ...  ",end="")
                if self.mergeType.outputFormat == "DataFrame":
                    if self.verbose: print(f"(Converting To DataFrame ... ",end="")
                    modValData = modValData.apply(Series)
                    if self.verbose: print("✓)  ...  ")
                elif self.mergeType.outputFormat == "Series":
                    pass
                self.dataio.saveFileTypeModValData(modVal, self.mergeType.outputName, modValData)
                if self.verbose: print("✓")
            else:
                if self.verbose: print(f"   ===> Not Saving {self.mergeType.output} ModVal={modVal} Data.")
            
        if self.verbose: ts.stop()