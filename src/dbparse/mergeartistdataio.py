""" Base Class For Merging Artist ModVal Data """

__all__ = ["MergeArtistDataIO"]

from dbbase import MusicDBRootDataIO, getModVals
from utils import Timestat
from pandas import Series
from .mergedataiobase import MergeDataIOBase
from .mergefiletype import MergeFileType


class MergeArtistDataIO(MergeDataIOBase):
    def __repr__(self):
        return f"MergeArtistDataIO(db={self.dataio.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, mergeType: MergeFileType, **kwargs):
        super().__init__(rdio, **kwargs)
        assert isinstance(mergeType, MergeFileType), f"mergeType [{mergeType}] is not a MergeFileType"
        assert isinstance(mergeType.inputName, list), "MergeArtistDataIO.inputName must be a list of inputNames"
        self.mergeType = mergeType
        
        if self.verbose:
            outputName = mergeType.outputName if isinstance(mergeType.outputName, str) else "ModVal"
            print(f"  {'MergeArtistDataIO:': <25} [{mergeType.inputName}] => [{outputName}]")
        
    ###########################################################################
    # Merge Sub Artist Data
    ###########################################################################
    def merge(self, modVal=None, force=False, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)
        test = kwargs.get('test', False)
        assert isinstance(modVal, int) or modVal is None, f"ModVal [{modVal}] is not an int/None"
        inputNames = self.mergeType.inputName
        assert isinstance(inputNames, list) and len(inputNames) > 0, f"Invalid inputName [{inputNames}]"
        modVals = getModVals(modVal)
        ts = Timestat(f"Merging {len(modVals)} Artist ModVal Data", verbose=verbose)

        mergeArgs = kwargs
        for n, modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                ts.update(n=n + 1, N=len(modVals))

            mergeStats = {"Start": {}, "Merged": {}, "Missing": {}, "End": 0}
            modValData = self.rdio.getData(f"ModVal{inputNames[0]}", modVal)
            cmt = f"MergeArtistDataIO expected an Artist dict object, but found a [{type(modValData)}]"
            assert isinstance(modValData, Series), cmt
            mergeStats["Start"][inputNames[0]] = len(modValData)
            
            for inputName in inputNames[1:]:
                inputModValData = self.rdio.getData(f"ModVal{inputName}", modVal)
                mergeStats["Start"][inputName] = len(modValData)
                mergeStats["Merged"][inputName] = 0
                mergeStats["Missing"][inputName] = 0
                if not isinstance(inputModValData, Series) or len(inputModValData) == 0:
                    continue
                for artistID, artistIDData in inputModValData.items():
                    if modValData.get(artistID) is None:
                        mergeStats["Missing"][inputName] += 1
                        continue
                    modValData[artistID].merge(artistIDData, **mergeArgs)
                    mergeStats["Merged"][inputName] += 1
                        
            mergeStats["End"] = len(modValData)
            
            if test is True:
                print("Only testing. Will not save...")
                continue
            
            self.mvdopt.save(modValData, modVal, self.mergeType, mergeStats)
            
        ts.stop()