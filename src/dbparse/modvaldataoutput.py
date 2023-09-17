""" Output Class For Parse/Merge """

__all__ = ["ModValDataOutput"]

from dbbase import MusicDBDataIO, MusicDBRootDataIO
from dbraw import RawDataIOBase
from pandas import Series, DataFrame
from .parsefiletype import ParseFileType
from .concatfiletype import ConcatFileType
from .mergefiletype import MergeFileType


class ModValDataOutput:
    def __repr__(self):
        return f"ModValDataOutput(db={self.dataio.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.rdio = rdio
        self.dataio = MusicDBDataIO(rdio, **kwargs)
        self.rdbiobase = RawDataIOBase()
        
    def showStats(self, stats):
        retval = "     ".join([f"{key} = {val}" for key, val in stats.items()]) if isinstance(stats, dict) else ""
        return retval
                
    def getOutputNameData(self, modValData, modVal, pmType):
        if isinstance(pmType, ParseFileType):
            raise ValueError("Is this ever called???")
            assert isinstance(self.modValData, dict), f"ModValData Output should be a dict because it came from Parse, but it's a [{type(modValData)}]"
            outputData = modValData.get(outputName)
            assert isinstance(outputData, dict), f"ModValData With OutputName [{outputName}] is not a dict, but a [{type(outputData)}]"
            if isinstance(modVal, int):
                retval = outputData.get(modVal)
                assert isinstance(retval, (DataFrame, Series)) or retval is None, f"ModValData With OutputName [{outputName}] and ModVal [{modVal}] is not a [DataFrame,Series,None], but a [{type(retval)}]"
            else:
                retval = outputData
                assert isinstance(retval, dict), f"ModValData With OutputName [{outputName}] and ModVal [{modVal}] is not a [dict], but a [{type(retval)}]"
        elif isinstance(pmType, MergeFileType):
            assert isinstance(modValData, (Series, DataFrame)), f"ModValData Output should be a [DataFrame,Series] because it came from Merge, but it's a [{type(modValData)}]"
            retval = modValData
        elif isinstance(pmType, ConcatFileType):
            assert isinstance(modValData, (Series, DataFrame)), f"ModValData Output should be a [DataFrame,Series] because it came from Concat, but it's a [{type(modValData)}]"
            retval = modValData
        else:
            raise ValueError(f"Parse/Merge FileType [{type(pmType)}] is not allowed")
        return retval
            
    def save(self, modValData, modVal, pmType, stats):
        assert isinstance(modValData, (Series, dict)), f"ModValData is not a [Series,dict], but a [{type(modValData)}]"
        assert isinstance(modVal, int), f"ModVal is not a [int], but a [{type(modVal)}]"
        assert isinstance(pmType, (ParseFileType, MergeFileType, ConcatFileType)), f"ParseMerge FileType is a [{type(pmType)}]"
        assert isinstance(stats, dict), f"Stats is not a [dict], but a [{type(stats)}]"
        outputInfo = pmType.getOutput()
        for outputName, (outputLevel, outputFormat) in outputInfo.items():
            if outputLevel == 0:
                # ModVal Output
                outputData = self.getOutputNameData(modValData, modVal, pmType=pmType)
                if self.verbose:
                    print(f"   ===> Saving ModVal={modVal} Data.  [{self.showStats(stats)}]  ...  ", end="")
                saveData = self.formatOutput(outputData, outputFormat)
                self.rdio.saveData("ModVal", modVal, data=saveData)
                if self.verbose:
                    print("✓")
            elif outputLevel == 1:
                # FileType ModVal Output
                outputData = self.getOutputNameData(modValData, modVal, pmType=pmType)
                if self.verbose:
                    print(f"   ===> Saving {outputName} ModVal={modVal} Data.  [{self.showStats(stats)}]  ...  ", end="")
                saveData = self.formatOutput(outputData, outputFormat)
                self.rdio.saveData(f"ModVal{outputName}", modVal, data=saveData)
                if self.verbose:
                    print("✓")
            elif outputLevel == 2:
                # Shuffle ModVal Output
                outputData = self.getOutputNameData(modValData, modVal=None, pmType=pmType)
                assert outputName.startswith("Shuffle"), f"outputName [{outputName}] needs to start with Shuffle"
                for shuffleModVal, shuffleData in outputData.items():
                    if self.verbose:
                        print(f"   ===> Saving {outputName} ModVal={modVal} Data.  [{self.showStats(stats)}]  ...  ", end="")
                    saveData = self.formatOutput(shuffleData, outputFormat)
                    self.rdio.saveData(f"{outputName}", shuffleModVal, modVal, data=saveData)
                    if self.verbose:
                        print("✓")
            else:
                raise ValueError(f"Output level with outputName == [{outputName}] must be 0, 1 or 2")
        
    def formatOutput(self, modValData: Series, outputFormat: str):
        def getMedia(rData):
            media = getattr(rData, "media") if hasattr(rData, "media") else None
            mediaCollection = media.get() if hasattr(media, "get") and callable(getattr(media, "get")) else None
            retval = {"Media": mediaCollection}
            return retval
        
        def getProfile(rData):
            profile = getattr(rData, "profile") if hasattr(rData, "profile") else None
            retval = profile.get() if hasattr(profile, "get") and callable(getattr(profile, "get")) else None
            return retval
        
        def getBasic(rData):
            basic = getattr(rData, "basic") if hasattr(rData, "basic") else None
            retval = basic.get() if hasattr(basic, "get") and callable(getattr(basic, "get")) else None
            return retval
        
        if outputFormat == "Series":
            modValData = Series(modValData) if isinstance(modValData, dict) else modValData
            if isinstance(modValData, Series):
                if self.verbose:
                    print(f"[{modValData.shape} => {modValData.shape}] ... ", end="")
                return modValData
        elif outputFormat == "DataFrame":
            modValData = Series(modValData) if isinstance(modValData, dict) else modValData
            assert isinstance(modValData, Series), f"Requiring a DataFrame, but modValData is not a Series [{type(modValData)}]"
            # Check Data Type For 1st 10 rows
            isArtistData = all([self.rdbiobase.isRawArtistData(rData) for rData in modValData.head(10).values])
            isMediaRootData = all([self.rdbiobase.isRawMediaRootData(rData) for rData in modValData.head(10).values])
            isMediaDeepData = all([self.rdbiobase.isRawMediaDeepData(rData) for rData in modValData.head(10).values])
            if isArtistData is True:
                basic = modValData.apply(getBasic)
                basic = DataFrame(basic.to_dict()).T
                profile = modValData.apply(getProfile)
                profile = DataFrame(profile.to_dict()).T
                media = modValData.apply(getMedia)
                media = DataFrame(media.to_dict()).T
                
                assert basic.count().min() > 0, "Output DataFrame does not have any Basic data filled..."
                retval = basic
                retval = retval.join(profile) if profile.count().max() > 0 else retval
                retval = retval.join(media) if media.count().max() > 0 else retval
                if self.verbose:
                    print(f"[{modValData.shape} => {retval.shape}] ... ", end="")
                return retval
            elif isMediaRootData is True:
                retval = modValData.apply(lambda rData: rData.get())
                retval = DataFrame(retval.to_dict()).T
                if self.verbose:
                    print(f"[{modValData.shape} => {retval.shape}] ... ", end="")
                return retval
            elif isMediaDeepData is True:
                retval = modValData.apply(lambda rData: rData.get())
                retval = DataFrame(retval.to_dict()).T
                if self.verbose:
                    print(f"[{modValData.shape} => {retval.shape}] ... ", end="")
                return retval
            else:
                raise TypeError(f"Unsure how to format modValData rData {[type(rData) for rData in modValData.head(3)]}")
        else:
            raise TypeError(f"Unsure how to format modValData of type [{type(modValData)}] and require [{outputFormat}]")
            