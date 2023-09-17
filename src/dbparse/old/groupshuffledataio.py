""" Class For Group(by) ModVal Shuffle Data """

__all__ = ["GroupShuffleDataIO"]

from base import RawDataIOBase, GroupFileType
from timeutils import Timestat
from pandas import concat, Series, DataFrame
from listUtils import getFlatList
from numpy import array_split, ndarray, int64
from .groupdataiobase import GroupDataIOBase    

class GroupShuffleDataIO(GroupDataIOBase):
    def __init__(self, mdbdata, mdbdir, groupType, **kwargs):
        super().__init__(mdbdata, mdbdir, **kwargs)        
        self.rawio      = RawDataIOBase()
        self.groupType  = groupType
        self.getModVal  = self.mv.getModVal
        self.mergeType  = "Shuffle"
                        
        if self.verbose:
            if isinstance(groupType.split,str):
                print(f"  {'GroupShuffleDataIO:': <25} [({groupType.inputName}) ⯒ ({groupType.by}) ({groupType.split}] => [{groupType.outputName}] Data")
            else:
                print(f"  {'GroupShuffleDataIO:': <25} [({groupType.inputName}) ⯒ ({groupType.by})] => [{groupType.outputName}] Data")

    
        
    ####################################################################################################################################################
    # Primary Grouping
    ####################################################################################################################################################
    def splitKey(self, key, split):
        if split is None:
            return key
        elif split == "ArtistID":
            assert key.count("-") > 0, f"Trying to split a key by {split}, but there is only one ID in key [{key}]"
            return key.split("-")[0]
        else:
            raise ValueError("Only ArtistID is allowed as a split value")
        
    def group(self, modVal=None, force=False):
        modVals        = self.getModVals(modVal)
        if self.verbose: ts = Timestat(f"Grouping {len(modVals)} Shuffle ModVal Data")
            
        """
        groupStats = {"ModVals": len(modVals), "Files": 0}
        modValData = None
        for modVal in modVals:
            files  = self.dataio.getNewShuffleFiles(modVal, self.groupType.inputName, force=force)
            if len(files) == 0:
                continue
            groupStats["Files"] += len(files)
            mvData = concat([self.io.get(ifile) for ifile in files])
            modValData = concat(modValData, mvData) if isinstance(modValData,(DataFrame,Series)) else mvData
                
        groupStats = {"Total": modValData.shape[0]}
        if isinstance(modValData,DataFrame):
            if self.groupType.by == "index":
                modValData.index = modValData.index.map(lambda artistID: (artistID, self.mv.getModVal(splitKey(artistID, groupType.split))))
                for modValGroup,modValGroupData in modValData.groupby(axis=0, level=1):
                    if modValGroupData.shape[0] == 0:
                        continue
                    modValGroupData = modValGroupData.droplevel(1)
                    groupStats["Slice"] = modValGroupData.shape[0]
                    self.saveOutput(modValGroup, modValGroupData, groupType, groupStats)
            else:
                assert self.groupType.by in modValData.columns, f"Groupby key [{self.groupType.by}] is not in modValData columns [{modValData.columns}]"
                modValData["ModVal"] = modValData[self.groupType.by].map(lambda artistID: self.mv.getModVal(splitKey(artistID, groupType.split)))
                for modValGroup,modValGroupData in modValData.groupby("ModVal"):
                    if modValGroupData.shape[0] == 0:
                        continue
                    modValGroupData = modValGroupData.drop(["ModVal"], axis=1)
                    groupStats["Slice"] = modValGroupData.shape[0]
                    self.saveOutput(modValGroup, modValGroupData, groupType, groupStats)
        elif isinstance(modValData,Series):
            if self.groupType.by == "index":
                modValData.index = modValData.index.map(lambda artistID: (artistID, self.mv.getModVal(splitKey(artistID, groupType.split))))
                for modValGroup,modValGroupData in modValData.groupby(axis=0, level=1):
                    if modValGroupData.shape[0] == 0:
                        continue
                    modValGroupData = modValGroupData.droplevel(1)
                    groupStats["Slice"] = modValGroupData.shape[0]
                    self.saveOutput(modValGroup, modValGroupData, groupType, groupStats)
            else:
                raise ValueError("Series can only be grouped by index in GroupModValShuffleData")
        else:
            raise TypeError(f"Unsure how to handle modValData of type [{type(modValData)}]")
                                
        """
        if self.verbose: ts.stop()

        
    def saveOutput(self, modValGroup, modValGroupData, groupType, groupStats, force=False):
        outputInfo = groupType.getOutput()
        for outputName,(outputLevel,outputFormat) in outputInfo.items():
            if outputLevel == 0:
                assert outputName is None, f"OutputName for OutputLevel==0 is not None [{outputName}]"
                if self.verbose: print(f"   ===> Saving ModVal={modValGroup} Data.  [{self.showStats(groupStats)}]  ...  ",end="")
                self.dataio.saveModValData(modValGroup, modValGroupData)
                if self.verbose: print("✓")
            elif outputLevel == 1:
                if self.verbose: print(f"   ===> Saving {outputName} ModVal={modValGroup} Data.  [{self.showStats(groupStats)}]  ...  ",end="")
                self.dataio.saveFileTypeModValData(modVal, modValGroup, modValGroupData)
                if self.verbose: print("✓")
            elif outputLevel == 2:
                raise ValueError(f"OutputLevel == 2 is not allowed yet for GroupModValShuffleData [OutputName={outputName}]")
            else:
                raise ValueError(f"Output level with outputName == [{outputName}] must be 0, 1 or 2")