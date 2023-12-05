""" Base Class For Merging Shuffle ModVal Data """

__all__ = ["MergeShuffleDataIO"]

from dbbase import MusicDBRootDataIO, getModVals
from utils import Timestat, FileIO
from pandas import Series
from .mergedataiobase import MergeDataIOBase
from .mergefiletype import MergeFileType


class MergeShuffleDataIO(MergeDataIOBase):
    def __repr__(self):
        return f"MergeShuffleDataIO(db={self.dataio.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, mergeType: MergeFileType, **kwargs):
        super().__init__(rdio, **kwargs)
        assert isinstance(mergeType, MergeFileType), f"mergeType [{mergeType}] is not a MergeFileType"
        # assert isinstance(mergeType.inputName, list) and len(mergeType.inputName) > 0, f"Invalid inputName [{mergeType.inputName}]"
        self.mergeType = mergeType
        
        if self.verbose:
            print(f"  {'MergeShuffleDataIO:': <25} [{mergeType.inputName}] => [{mergeType.outputName}]")
                
    ###########################################################################
    # Merge Sub Artist Data
    ###########################################################################
    def merge(self, modVal=None, force=False, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)
        test = kwargs.get('test', False)
        assert isinstance(modVal, int) or modVal is None, f"ModVal [{modVal}] is not an int/None"
        modVals = getModVals(modVal)
        ts = Timestat(f"Merging {len(modVals)} Shuffle ModVal Data", verbose=verbose)
        
        io = FileIO()
        
        for n, modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                ts.update(n=n + 1, N=len(modVals))

            mergeStats = {"Start": 0, "Files": 0, "Merged": 0, "New": 0, "End": 0}
            files = self.getShuffleArtistFiles(modVal, self.mergeType, **kwargs)
            if len(files) == 0:
                print("  No files to merge. Returning.")
                ts.stop()
                return
            files = files[:2] if test is True else files

            modValData = {}
            mergeStats["Start"] = len(modValData)
            for ifile in files:
                mergeStats["Files"] += 1
                inputModValData = io.get(ifile)
                assert isinstance(inputModValData, Series), f"File data is not a Series [{type(inputModValData)}] from [{ifile}]"
                for artistID, artistIDData in inputModValData.items():
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
            
            if test is True:
                print("Only testing. Will not save...")
                continue
            
            if mergeStats["Files"] > 0:
                return self.mvdopt.save(modValData, modVal, self.mergeType, mergeStats)
            else:
                if self.verbose:
                    print(f"   ===> Not Saving {self.mergeType.outputName} ModVal={modVal} Data.")
            
        ts.stop()