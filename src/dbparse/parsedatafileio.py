""" Base Output Class For Parsing Data """

__all__ = ["ParseDataFileIO"]

from dbbase import MusicDBRootDataIO, MusicDBDataIO, MusicDBIDModVal
from dbraw import RawDataIOBase
from pandas import Series, DataFrame
from .parsefiletype import ParseFileType
from .parsestats import ParseStats
from .groupdatadirio import RawDataDirIO

                             
################################################################################
# Parse File I/O (to be used in Parse Data IO)
################################################################################
class ParseDataFileIO:
    def __repr__(self):
        return f"ParseDataFileIO(db={self.dataio.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, rawio: RawDataIOBase, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.rddio = RawDataDirIO(rdio, **kwargs)
        self.dataio = MusicDBDataIO(rdio, **kwargs)
        self.rdio = rdio
        self.rawio = rawio
        self.rdbiobase = RawDataIOBase()
        self.modValData = None
        self.mv = MusicDBIDModVal()
            
    ###########################################################################
    # Get End Stats
    ###########################################################################
    def getStartStats(self, parseStats: ParseStats) -> 'None':
        assert isinstance(self.modValData, dict), f"ModValData [{type(self.modValData)}] is not set as a dict yet!"
        return
        parseStats.start = {outputName: len(inputData) for outputName, inputData in self.modValData.items()}
        
    def getEndStats(self, parseStats: ParseStats):
        assert isinstance(self.modValData, dict), f"ModValData [{type(self.modValData)}] is not set as a dict yet!"
        parseStats.output = {outputName: len(outputData) for outputName, outputData in self.modValData.items()}
    
    ###########################################################################
    # Pretty Print Stats
    ###########################################################################
    def showStats(self, stats):
        retval = "     ".join([f"{key} = {val}" for key, val in stats.items()]) if isinstance(stats, dict) else ""
        return retval
        
    ###########################################################################
    # Update ModVal Data Helper Functions
    ###########################################################################
    def updateModValData(self, rData, parseType):
        assert isinstance(rData, dict), f"Expected a dict rData, but it is type [{type(rData)}]"
        outputTypeData = parseType.getOutput()
        assert len(outputTypeData) == len(rData), f"OutputTypeData [{outputTypeData.keys()}] != [{rData.keys()}]"
        
        status = {}
        for outputName, outputInfo in outputTypeData.items():
            rDataOutput = rData.get(outputName)
            if self.rdbiobase.isRawArtistData(rDataOutput):
                status[outputName] = self.updateModValDataRaw(key=outputName, rDataOutput=rDataOutput)
            elif self.rdbiobase.isRawMediaDeepData(rDataOutput) or self.rdbiobase.isRawMediaRootData(rDataOutput):
                status[outputName] = self.updateModValDataMedia(key=outputName, rDataOutput=rDataOutput)
            elif isinstance(rDataOutput, dict):
                status[outputName] = self.updateModValDataDict(key=outputName, rDataOutput=rDataOutput)
            elif rDataOutput is None:
                status[outputName] = False
            else:
                raise ValueError(f"Unsure how to handle rDataOutput of type [{type(rDataOutput)}] for OutputName [{outputName}]")
        
        retval = all(status.values())
        return retval
        
    def updateModValDataDict(self, key, rDataOutput):
        status = self.assignModValData(key, {(rDataID, self.mv.getModVal(rDataID)): rData for rDataID, rData in rDataOutput.items()})
        return status
            
    def updateModValDataRaw(self, key, rDataOutput):
        status = self.assignModValData(key, {(rDataOutput.basic.id, self.mv.getModVal(rDataOutput.basic.id)): rDataOutput})
        return status
            
    def updateModValDataMedia(self, key, rDataOutput):
        status = self.assignModValData(key, {(rDataOutput.mediaID, self.mv.getModVal(rDataOutput.mediaID)): rDataOutput})
        return status
                        
    def assignModValData(self, key, modValDataToAssign):
        status = {}
        for (rDataID, rDataModVal), rData in modValDataToAssign.items():
            status[rDataID] = isinstance(rDataID, str) & isinstance(rDataModVal, int) & (rData is not None)
            if not status[rDataID]:
                continue
            if self.modValData[key].get(rDataModVal) is None:
                self.modValData[key][rDataModVal] = {}
            if self.modValData[key][rDataModVal].get(rDataID) is not None:
                rDataToKeep = self.compareUpdateData(existingData=self.modValData[key][rDataModVal][rDataID], updateData=rData, onError=True)
                self.modValData[key][rDataModVal][rDataID] = rDataToKeep
            else:
                self.modValData[key][rDataModVal][rDataID] = rData
        return all(status.values())
            
    def compareUpdateData(self, existingData, updateData, onError=False):
        if self.rdbiobase.isRawArtistData(existingData) and self.rdbiobase.isRawMediaCollectionData(updateData):
            existingData.summary()
            updateData.summary()
            raise ValueError("Stopping Due To Duplicates")
        elif all([self.rdbiobase.isRawArtistData(obj) for obj in [existingData, updateData]]):
            existingData.merge(updateData)
        elif all([self.rdbiobase.isRawMediaCollectionData(obj) for obj in [existingData, updateData]]):
            existingData.merge(updateData)
        elif all([self.rdbiobase.isRawMediaDeepData(obj) | self.rdbiobase.isRawMediaRootData(obj) for obj in [existingData, updateData]]):
            mutils = getattr(self.rawio, "mutils") if hasattr(self.rawio, "mutils") else None
            if hasattr(mutils, "compareMedia") and callable(getattr(mutils, "compareMedia")):
                if mutils.compareMedia(existingData, updateData) is False:
                    print("================= Found duplicate non-overlapping MediaDeepData objects =================")
                    print(existingData)
                    print(updateData)
                    raise ValueError("Stopping Due To Duplicates Found From MediaUtils")
            else:
                if existingData.compare(existingData) is not True:
                    if onError is True:
                        print("================= Found duplicate non-overlapping MediaDeepData objects =================")
                        print(existingData)
                        print(updateData)
                        raise ValueError("Stopping Due To Duplicates")
        else:
            raise TypeError(f"Unsure how to compare existing [{type(existingData)}] and update [{type(updateData)}]")

        return existingData
    
    ###########################################################################
    # I/O Functions
    ###########################################################################
    def getInput(self, modVal, parseType, parseStats: ParseStats, force=False) -> 'None':
        self.setInputData(modVal, parseType, parseStats, force)
        
    def setInputData(self, modVal, parseType, parseStats: ParseStats, force=False) -> 'None':
        if isinstance(parseType.outputName, list):
            modValData = {outputName: {} for outputName in parseType.outputName}
        elif isinstance(parseType.outputName, str):
            inputData = self.dataio.getFileTypeModValData(modVal, parseType.outputName, force=force)
            inputData = inputData.to_dict() if isinstance(inputData, Series) else inputData
            modValData = {parseType.outputName: inputData}
        elif parseType.outputName is None:
            modValData = {parseType.outputName: {}}
        else:
            raise TypeError(f"Unsure about outputName type [{type(parseType.outputName)}]")

        self.modValData = modValData
        self.getStartStats(parseStats)
        assert isinstance(modValData, dict), f"modValData [{type(modValData)}] is not a dict"
        
    def formatOutput(self, modValData, outputFormat):
        if outputFormat == "Series":
            modValData = Series(modValData) if isinstance(modValData, dict) else modValData
            if isinstance(modValData, Series):
                return modValData
        elif outputFormat == "DataFrame":
            modValData = Series(modValData) if isinstance(modValData, dict) else modValData
            assert isinstance(modValData, Series), f"Requiring a DataFrame, but modValData is not a Series [{type(modValData)}]"
            
            # Check Data Type For 1st 10 rows
            isArtistData = all([self.rdbiobase.isRawArtistData(rData) for rData in modValData.head(10).values])
            isMediaRootData = all([self.rdbiobase.isRawMediaRootData(rData) for rData in modValData.head(10).values])
            isMediaDeepData = all([self.rdbiobase.isRawMediaDeepData(rData) for rData in modValData.head(10).values])
            
            if isArtistData is True:
                basic = modValData.apply(lambda rData: getattr(rData, "basic").get() if hasattr(rData, "basic") else None)
                basic = DataFrame(basic.to_dict()).T
                profile = modValData.apply(lambda rData: getattr(rData, "profile").get() if hasattr(rData, "profile") else None)
                profile = DataFrame(profile.to_dict()).T
                media = modValData.apply(lambda rData: {"Media": rData.media.get()} if hasattr(rData, "media") else None)
                media = DataFrame(media.to_dict()).T

                assert basic.count().min() > 0, "Output DataFrame does not have any Basic data filled..."
                retval = basic
                retval = retval.join(profile) if profile.count().max() > 0 else retval
                retval = retval.join(media) if media.count().max() > 0 else retval
                return retval
            elif isMediaRootData is True:
                retval = modValData.apply(lambda rData: rData.get())
                retval = DataFrame(retval.to_dict()).T
                return retval
            elif isMediaDeepData is True:
                retval = modValData.apply(lambda rData: rData.get())
                retval = DataFrame(retval.to_dict()).T
                return retval
            else:
                raise TypeError(f"Unsure how to format modValData rData {[type(rData) for rData in modValData.head(3)]}")
        else:
            raise TypeError(f"Unsure how to format modValData of type [{type(modValData)}] and require [{outputFormat}]")

    def saveOutput(self, modVal: int, parseType: ParseFileType, pstats: ParseStats, force=False) -> 'None':
        outputInfo = parseType.getOutput()
        for outputName, (outputLevel, outputFormat) in outputInfo.items():
            if outputLevel == 0:
                assert outputName is None, f"OutputName for OutputLevel==0 is not None [{outputName}]"
                assert len(self.modValData[outputName]) == 1, f"More than one modVal in primary ModValData | {self.modValData[outputName].keys()}"
                assert self.modValData[outputName].get(modVal) is not None, f"Did not find [{modVal}] in primary ModValData | {self.modValData[outputName].keys()}"
                
                outputData = self.modValData[outputName][modVal]
                if self.verbose:
                    print(f"   ===> Saving ModVal={modVal} Data.  [pstats.showBasic()]  ...  ", end="")
                saveData = self.formatOutput(outputData, outputFormat)
                self.rdio.saveData("ModVal", modVal, data=saveData)
                #self.dataio.saveModValData(modVal, saveData)
                if self.verbose:
                    print("✓")
            elif outputLevel == 1:
                assert len(self.modValData[outputName]) == 1, f"More than one modVal in primary ModValData | {self.modValData[outputName].keys()}"
                assert self.modValData[outputName].get(modVal) is not None, f"Did not find [{modVal}] in primary ModValData | {self.modValData[outputName].keys()}"
                
                outputData = self.modValData[outputName][modVal]
                saveData = self.formatOutput(outputData, outputFormat)
                if self.verbose:
                    print(f"   ===> Saving {outputName} ModVal={modVal} Data.  [{pstats.showBasic()}]  ...  ", end="")
                self.rdio.saveData(f"ModVal{outputName}", modVal, data=saveData)
                #self.dataio.saveFileTypeModValData(modVal, outputName, saveData)
                if self.verbose:
                    print("✓")
            elif outputLevel == 2:
                outputData = self.modValData[outputName]
                assert isinstance(outputData, dict), "Output ModValData is not formatted correctly"
                assert outputName.startswith("Shuffle"), f"outputName [{outputName}] needs to start with Shuffle"
                
                if self.verbose:
                    print(f"   ===> Saving {outputName} ModVal={modVal} Data.  [{pstats.showBasic()}]  ...  ", end="")
                for shuffleModVal, shuffleData in outputData.items():
                    saveData = self.formatOutput(shuffleData, outputFormat)
                    self.rdio.saveData(f"{outputName}", shuffleModVal, modVal, data=saveData)
                    #self.dataio.saveShuffleData(shuffleModVal, modVal, outputName, saveData)
                if self.verbose:
                    print("✓")
            else:
                raise ValueError(f"Output level with outputName == [{outputName}] must be 0, 1 or 2")
                