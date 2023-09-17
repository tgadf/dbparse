""" Base Class For Concating Shuffle ModVal Data """

__all__ = ["ConcatShuffleDataIO"]

from dbbase import MusicDBRootDataIO, getModVals
from utils import Timestat, FileIO
from pandas import concat, Series
from .concatdataiobase import ConcatDataIOBase
from .concatfiletype import ConcatFileType


class ConcatShuffleDataIO(ConcatDataIOBase):
    def __repr__(self):
        return f"ConcatShuffleDataIO(db={self.db}, concatType={self.concatType})"
    
    def __init__(self, rdio: MusicDBRootDataIO, concatType, **kwargs):
        super().__init__(rdio, **kwargs)
        self.concatType = concatType
        
        if self.verbose:
            outputName = concatType.outputName if isinstance(concatType.outputName, str) else "ModVal"
            print(f"  {'ConcatShuffleDataIO:': <25} [{concatType.inputName}] => [{outputName}]")
        
    ####################################################################################################
    # concat Sub Shuffle Data
    ####################################################################################################
    def concat(self, modVal=None, **kwargs) -> 'None':
        verbose = kwargs.get('verbose', self.verbose)
        test = kwargs.get('test', False)
        assert isinstance(modVal, int) or modVal is None, f"ModVal [{modVal}] is not an int/None"
        assert isinstance(self.concatType, ConcatFileType), f"concatType [{self.concatType}] is not a ConcatFileType"
        
        modVals = getModVals(modVal)
        ts = Timestat(f"Concating {len(modVals)} Shuffle ModVal Data", verbose=verbose)
        
        io = FileIO()

        for n, modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                ts.update(n=n + 1, N=len(modVals))

            concatStats = {"Start": 0, "Files": 0, "Duplicates": 0, "End": 0}
            files = self.getShuffleFiles(modVal, self.concatType, **kwargs)
            if len(files) == 0:
                print("  No files to concat. Returning.")
                ts.stop()
                return
            files = files[:2] if test is True else files
                        
            modValData = None
            for ifile in files:
                concatStats["Files"] += 1
                inputModValData = io.get(ifile)
                assert isinstance(inputModValData, Series), f"File data is not a Series [{type(inputModValData)}] from [{ifile}]"
                modValData = concat([modValData, inputModValData]) if isinstance(modValData, Series) else inputModValData
            assert isinstance(modValData, Series), f"ConcatShuffleDataIO expected an Shuffle Series object, but found a [{type(modValData)}]"
            concatStats["Duplicates"] = modValData.index.duplicated().sum()
            modValData = modValData[~modValData.index.duplicated()]
            concatStats["End"] = len(modValData)
            
            if test is True:
                print("Only testing. Will not save...")
                continue
            
            if concatStats["End"] > 0 and concatStats["Files"] > 0:
                self.mvdopt.save(modValData, modVal, self.concatType, concatStats)
            else:
                if self.verbose:
                    print(f"   ===> Not Saving {self.concatType.outputName} ModVal={modVal} Data.")
            
        ts.stop()