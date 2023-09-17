""" Base Class For Concating Artist ModVal Data """

__all__ = ["ConcatArtistDataIO"]

from dbbase import ConcatFileType, RawDataIOBase
from utils import Timestat
from pandas import concat, Series
from numpy import array_split, ndarray, int64
from .concatdataiobase import ConcatDataIOBase


class ConcatArtistDataIO(ConcatDataIOBase):
    def __init__(self, mdbdata, mdbdir, concatType, **kwargs):
        super().__init__(mdbdata, mdbdir, **kwargs)
        self.concatType = concatType
        self.rdbio = RawDataIOBase()
        assert isinstance(concatType.inputName, list), f"ConcatArtistDataIO.inputName must be a list of inputNames"
        
        if self.verbose:
            outputName = concatType.outputName if isinstance(concatType.outputName,str) else "ModVal"
            print(f"  {'ConcatArtistDataIO:': <25} [{concatType.inputName}] => [{outputName}]")
        
    ###################################################################################################################
    # concat Sub Artist Data
    ###################################################################################################################
    def concat(self, modVal=None, force=False):
        modVals        = self.getModVals(modVal)
        if self.verbose: ts = Timestat(f"Concating {len(modVals)} Artist ModVal Data")

        for n,modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                if self.verbose: ts.update(n=n+1,N=len(modVals))

            concatStats = {"Start": 0, "Concat": [], "End": 0}
            inputNames = self.concatType.inputName
            modValData = None
            for inputName in inputNames:
                inputModValData = self.dataio.getFileTypeModValData(modVal, inputName, force=False)
                concatStats["Concat"].append(inputModValData.shape[0])
                modValData = concat([modValData,inputModValData]) if isinstance(modValData,Series) else inputModValData
            assert isinstance(modValData,Series), f"ConcatArtistDataIO expected an Artist Series object, but found a [{type(modValData)}]"
            concatStats["End"] = len(modValData)
            if concatStats["End"] > 0:
                self.mvdopt.save(modValData, modVal, self.concatType, concatStats)
            else:
                if self.verbose: print(f"   ===> Not Saving {self.concatType.outputName} ModVal={modVal} Data.")
            
        if self.verbose: ts.stop()