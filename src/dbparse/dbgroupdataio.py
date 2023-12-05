""" Parse/Merge/Concat (Group) Data IO """

__all__ = ["MusicDBGroupDataIO"]

from dbbase import MusicDBParamsBase, MusicDBRootDataIO, getModVals
from dbbase import MusicDBDir, MusicDBData
from .parsefiletype import ParseFileType
from .concatfiletype import ConcatFileType
from .mergefiletype import MergeFileType


###############################################################################
# Parse/Merge/Concat (Group) Data IO
###############################################################################
class MusicDBGroupDataIO:
    def __repr__(self):
        return f"MusicDBGroupDataIO(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, params: MusicDBParamsBase, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        assert isinstance(params, MusicDBParamsBase), f"params [{params}] is not a MusicDBParamsBase"
        self.mkDirs = kwargs.get('mkDirs', False)
        self.rdio = rdio
        self.db = self.rdio.db

        self.parseMap = params.parseMap
        assert isinstance(self.parseMap, dict), f"ParseMap for db={self.db} is not a dict [{type(self.parseMap)}]"
        self.mergeMap = params.mergeMap
        assert isinstance(self.mergeMap, dict), f"MergeMap for db={self.db} is not a dict [{type(self.mergeMap)}]"
        self.concatMap = params.concatMap
        assert isinstance(self.concatMap, dict), f"ConcatMap for db={self.db} is not a dict [{type(self.concatMap)}]"
        
        if self.verbose:
            print(self.__repr__())
        
    #######################################################################
    # Add GroupData To RootData
    #######################################################################
    def addGroupData(self) -> 'None':
        self.addParseInputData()
        self.addParseOutputData()
        self.addConcatOutputData()
        self.addMergeOutputData()
        self.addMergeInputData()  # must run the output before the input...

    #######################################################################
    # Raw ParseType Inputs
    #######################################################################
    def addParseInputData(self) -> 'None':
        for parseTypeKey, parseType in self.parseMap.items():
            assert isinstance(parseType, ParseFileType), f"parseType [{parseType}] is not a ParseFileType"
                
            inputNames = []
            if isinstance(parseType.inputName, str) and parseType.inputName not in ["SearchArtist"]:
                inputNames = [parseType.inputName]
            elif isinstance(parseType.inputName, list):
                inputNames = parseType.inputName
        
            for inputName in inputNames:
                key = f"Raw{inputName}ModVal"
                dbdir = self.rdio.getDBDir("RawModVal")
                datadir = MusicDBDir(path=dbdir, child=f"{inputName.lower()}s")
                self.rdio.addDir(key, datadir)
                if self.mkDirs is True:
                    for modVal in getModVals():
                        self.rdio.getDBDir(key).mkDir(modVal)
                    
                key = f"Raw{inputName}"
                if not self.rdio.isData(key):
                    datadir = self.rdio.getDBDir(f"Raw{inputName}ModVal")
                    dbdata = MusicDBData(path=datadir, arg=True)
                    self.rdio.addData(key, dbdata, addname=True)
                
                key = f"Raw{inputName}ModVal"
                if not self.rdio.isData(key):
                    datadir = self.rdio.getDBDir(f"Raw{inputName}ModVal")
                    dbdata = MusicDBData(path=datadir, arg=True)
                    self.rdio.addData(key, dbdata, addname=True)

    #######################################################################
    # Shuffle/ModVal ParseType Outputs
    #######################################################################
    def addParseOutputData(self) -> 'None':
        for parseTypeKey, parseType in self.parseMap.items():
            assert isinstance(parseType, ParseFileType), f"parseType [{parseType}] is not a ParseFileType"
            
            parseOutput = parseType.getOutput(level=1)
            assert isinstance(parseOutput, dict), f"parseOutput [{parseOutput}] is not a dict"
            for outputName, (outputLevel, outputFormat) in parseOutput.items():
                key = f"ModVal{outputName}"
                dbdir = self.rdio.getDBDir("ModVal")
                datadir = MusicDBDir(path=dbdir, child=f"{outputName.lower()}")
                self.rdio.addDir(key, datadir)
                if self.mkDirs is True:
                    self.rdio.getDBDir(key).mkDir()
                
                key = f"ModVal{outputName}"
                datadir = self.rdio.getDBDir(key)
                dbdata = MusicDBData(path=datadir, arg=True, suffix="DB")
                self.rdio.addData(key, dbdata, addname=True)
                
            parseOutput = parseType.getOutput(level=2)
            assert isinstance(parseOutput, dict), f"parseOutput [{parseOutput}] is not a dict"
            for outputName, (outputLevel, outputFormat) in parseOutput.items():
                key = f"{outputName}Base"
                dbdir = self.rdio.getDBDir("ModVal")
                datadir = MusicDBDir(path=dbdir, child=f"{outputName.lower()}")
                self.rdio.addDir(key, datadir)
                if self.mkDirs is True:
                    self.rdio.getDBDir(key).mkDir()
                
                key = outputName
                dbdir = self.rdio.getDBDir(f"{outputName}Base")
                datadir = MusicDBDir(path=dbdir, arg=True)
                self.rdio.addDir(key, datadir)
                if self.mkDirs is True:
                    for modVal in getModVals():
                        self.rdio.getDBDir(key).mkDir(modVal)
                
                key = outputName
                datadir = self.rdio.getDBDir(f"{outputName}")
                dbdata = MusicDBData(path=datadir, arg=True, suffix=f"{outputName}-DB")
                self.rdio.addData(key, dbdata, addname=True)
                        
    #######################################################################
    # Shuffle/ModVal Concat Output
    #######################################################################
    def addConcatOutputData(self) -> 'None':
        for concatTypeKey, concatType in self.concatMap.items():
            if not isinstance(concatType, ConcatFileType):
                continue
                
            concatOutput = concatType.getOutput()
            assert isinstance(concatOutput, dict), f"concatOutput [{concatOutput}] is not a dict"
            for concatOutputName, (concatOutputLevel, concatOutputFormat) in concatOutput.items():
                if concatOutputLevel == 0:
                    continue
                assert isinstance(concatOutputName, str), f"ConcatOutputName [{concatOutputName}] is not a string"
    
                key = f"ModVal{concatOutputName}"
                dbdir = self.rdio.getDBDir("ModVal")
                datadir = MusicDBDir(path=dbdir, child=f"{concatOutputName.lower()}")
                self.rdio.addDir(key, datadir)
                if self.mkDirs is True:
                    self.rdio.getDBDir(key).mkDir()
    
                key = f"ModVal{concatOutputName}"
                datadir = self.rdio.getDBDir(f"ModVal{concatOutputName}")
                dbdata = MusicDBData(path=datadir, arg=True, suffix="DB")
                self.rdio.addData(key, dbdata, addname=True)

    #######################################################################
    # Shuffle/ModVal Merge Inputs (assert existance because it should be created in previous step)
    #######################################################################
    def addMergeInputData(self) -> 'None':
        mergeChecks = []
        for mergeTypeKey, mergeType in self.mergeMap.items():
            assert isinstance(mergeType, (MergeFileType, dict)), f"mergeType [{mergeType}] is not a (MergeFileType, dict)"
            
            if isinstance(mergeType, MergeFileType):
                inputNames = mergeType.inputName if isinstance(mergeType.inputName, list) else [mergeType.inputName]
                mergeChecks.append([mergeTypeKey, mergeType.inputType, inputNames])
            elif isinstance(mergeType, dict):
                for mergeSubTypeKey, mergeSubType in mergeType.items():
                    if isinstance(mergeSubType, MergeFileType):
                        inputNames = mergeSubType.inputName if isinstance(mergeSubType.inputName, list) else [mergeSubType.inputName]
                        mergeChecks.append([mergeSubTypeKey, mergeSubType.inputType, inputNames])
                    elif isinstance(mergeSubType, list):
                        for mergeSubSubType in mergeSubType:
                            if isinstance(mergeSubSubType, MergeFileType):
                                inputNames = mergeSubSubType.inputName if isinstance(mergeSubSubType.inputName, list) else [mergeSubSubType.inputName]
                                mergeChecks.append([mergeSubTypeKey, mergeSubSubType.inputType, inputNames])
                            else:
                                raise TypeError(f"MergeSubTypeKey [{mergeSubTypeKey}] list entry is not a MergeFileType [{type(mergeSubSubType)}] object")
    
        for (mergeTypeKey, inputType, inputNames) in mergeChecks:
            for inputName in inputNames:
                dbKey = f"ModVal{inputName}" if inputType in ["Artist", "ArtistOR", "Media", "Profile"] else f"{inputName}"
                dbdir = self.rdio.getDBDir(dbKey)
                assert isinstance(dbdir, MusicDBDir), f"MergeTypeKey [{mergeTypeKey}] input [{dbKey}] is not previously defined in ParseMap"

    #######################################################################
    # Shuffle/ModVal Merge Output
    #######################################################################
    def addMergeOutputData(self) -> 'None':
        for mergeTypeKey, mergeType in self.mergeMap.items():
            assert isinstance(mergeType, MergeFileType), f"mergeType [{mergeType}] is not a MergeFileType"
            
            mergeOutput = mergeType.getOutput()
            assert isinstance(mergeOutput, dict), f"mergeOutput [{mergeOutput}] is not a dict"
            for mergeOutputName, (mergeOutputLevel, mergeOutputFormat) in mergeOutput.items():
                if mergeOutputLevel == 0:
                    continue
                assert isinstance(mergeOutputName, str), f"MergeOutputName [{mergeOutputName}] is not a string"
                
                key = f"ModVal{mergeOutputName}"
                dbdir = self.rdio.getDBDir("ModVal")
                datadir = MusicDBDir(path=dbdir, child=f"{mergeOutputName.lower()}")
                self.rdio.addDir(key, datadir)
                if self.mkDirs is True:
                    self.rdio.getDBDir(key).mkDir()
                
                key = f"ModVal{mergeOutputName}"
                datadir = self.rdio.getDBDir(f"ModVal{mergeOutputName}")
                dbdata = MusicDBData(path=datadir, arg=True, suffix="DB")
                self.rdio.addData(key, dbdata, addname=True)
            
    def addSearchData(self, key: str, fname: str) -> 'None':
        datadir = self.rdio.getDBDir("RawSearch")
        dbdata = MusicDBData(path=datadir, fname=fname)
        self.rdio.addData(key, dbdata, addname=True)
            
    def addSearchArgData(self, key: str, prefix: str) -> 'None':
        datadir = self.rdio.getDBDir("RawSearch")
        dbdata = MusicDBData(path=datadir, arg=True, prefix=prefix)
        self.rdio.addData(key, dbdata, addname=True)
