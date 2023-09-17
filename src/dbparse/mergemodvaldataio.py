""" Base Class For Merging Final ModVal Data """

__all__ = ["MergeModValDataIO"]

from dbbase import MusicDBRootDataIO
from dbraw import RawDataIOBase
from utils import Timestat, getFlatList
from pandas import concat, Series, DataFrame
from numpy import array_split, ndarray, int64
from .mergedataiobase import MergeDataIOBase
from .mergefiletype import MergeFileType


class MergeModValDataIO(MergeDataIOBase):
    def __init__(self, rdio: MusicDBRootDataIO, mergeType, **kwargs):
        super().__init__(rdio, **kwargs)
        
        self.rawio      = RawDataIOBase()
        self.mergeType  = {}
        mergeTypeKey    = "ModVal"
        numSlices       = {}
        assert isinstance(mergeType,dict), f"{mergeTypeKey} MergeType is not defined!"
        
        modValArtistMergeType  = mergeType.get("Artist")
        modValArtistMergeTypes = [modValArtistMergeType] if isinstance(modValArtistMergeType,MergeFileType) else modValArtistMergeType
        assert isinstance(modValArtistMergeTypes,list), f"{mergeTypeKey} Artist MergeType is type [{type(modValArtistMergeTypes)}] and not a list"
        numSlices["Artist"]   = 1
        
        modValMediaMergeTypes = mergeType.get("Media")
        modValMediaMergeTypes = [] if modValMediaMergeTypes is None else modValMediaMergeTypes
        assert isinstance(modValMediaMergeTypes,list), f"{mergeTypeKey} Media MergeType is type [{type(modValMediaMergeTypes)}] and not a list"
        numSlices["Media"]   = [numSlices["Artist"] for _ in range(len(modValMediaMergeTypes))]
        
        modValProfileMergeTypes = mergeType.get("Profile")
        modValProfileMergeTypes = [] if modValProfileMergeTypes is None else modValProfileMergeTypes
        assert isinstance(modValProfileMergeTypes,list), f"{mergeTypeKey} Profile MergeType is type [{type(modValProfileMergeTypes)}] and not a list"
        numSlices["Profile"]   = [numSlices["Artist"] for _ in range(len(modValProfileMergeTypes))]
            
        self.numSlices = numSlices
        self.mergeType = {}
        self.mergeType["Artist"]  = modValArtistMergeTypes
        self.mergeType["Media"]   = modValMediaMergeTypes
        self.mergeType["Profile"] = modValProfileMergeTypes
        
        if self.verbose:
            artistStr  = ", ".join([mergeType.inputName for mergeType in modValArtistMergeTypes])
            mergeStr   = ", ".join([mergeType.mediaName for mergeType in modValMediaMergeTypes+modValProfileMergeTypes])
            print(f"  {'MergeModValDataIO:': <25} [({artistStr}) ⟕ ({mergeStr})] => [ModVal] Data")

    
        
    #################################################################################################
    # Primary Merge
    #################################################################################################
    def merge(self, modVal=None, **kwargs):
        force = kwargs.get('force', False)
        if self.verbose: print(f"{'='*75} Merging ModValData (modVal={modVal}, force={force}) {'='*75}")
        
        #################################################################################################
        # Determine What We Need To Run
        #################################################################################################
        modVals = self.getMergeModVals(modVal, force=force)
        if len(modVals) == 0:
            return
        self.setSliceValues(modVals)
        
        if self.verbose: ts = Timestat(f"Merging {len(modVals)} ModVal Data")
        
        mergeStats      = {"ModValTotal": 0, "SliceTotal": 0, "Total": 0}
        numModIters     = len(self.sliceValues["Artist"])
        for i,sliceNum in enumerate(range(numModIters)):
            #################################################################################################
            artistData = self.loadArtistModValData(sliceNum=sliceNum)
            artistData = self.joinArtistMediaModValData(artistData=artistData, sliceNum=sliceNum)
            #################################################################################################

                
            ####### Saving ModVal Data #######
            artistData.index = artistData.index.map(lambda artistID: (artistID,self.mv.getModVal(artistID)))
            for modValGroup,modValGroupData in artistData.groupby(axis=0, level=1):
                if modValGroupData.shape[0] == 0:
                    continue
                modValGroupData = modValGroupData.droplevel(1)
                mergeStats["ModValTotal"] = len(modValGroupData)
                mergeStats["SliceTotal"] += len(modValGroupData)
                mergeStats["Total"]      += len(modValGroupData)
                if self.verbose: print(f"   ===> Saving ModVal={modValGroup} Data.  [{self.showStats(mergeStats)}]  ...  ",end="")
                if self.dataio.existsModValData(modValGroup) and False:
                    if self.verbose: print("(Merging With Old ... ",end="")
                    modValGroupData = concat([self.dataio.getModValData(modValGroup), modValGroupData])
                    modValGroupData = modValGroupData[~modValGroupData.index.duplicated()]
                    if self.verbose: print(f"{mergeStats['Total']} => {len(modValGroupData)}) ... ")
                self.dataio.saveModValData(modValGroup, modValGroupData)
                if self.verbose: print("✓")
            
            mergeStats["SliceTotal"] = 0
            if self.verbose: ts.update(n=i+1, N=self.numSlices)

        if self.verbose: ts.stop()
        
        
    #################################################################################################
    #
    # Helper Functions
    #
    #################################################################################################
    def getAvailableModVals(self, mergeType, force=False):
        if not isinstance(mergeType,MergeFileType):
            return []
        availableModVals = {modVal: self.dataio.getNewMergedFiles(modVal=modVal, mergeType=mergeType, force=force, verbose=False) for modVal in self.getModVals(None)}
        retval = [modVal for modVal,modValFiles in availableModVals.items() if len(modValFiles) > 0]
        return retval
    
    def getMergeModVals(self, modVal, force=False):
        userModVals = set(self.getModVals(modVal))
        availableModVals = {}
        for artistType in self.mergeType["Artist"]:
            availableModVals[artistType.inputName] = self.getAvailableModVals(artistType, force=force)
        for mediaType in self.mergeType["Media"]:
            availableModVals[mediaType.inputName] = self.getAvailableModVals(mediaType, force=force)
        for profileType in self.mergeType["Profile"]:
            availableModVals[profileType.inputName] = self.getAvailableModVals(profileType, force=force)
        availableModVals = set(getFlatList(availableModVals.values()))
        if self.verbose: print(f"  ===>> User ({len(userModVals)}) / Available ({len(availableModVals)})")
        modVals = sorted(list(userModVals.intersection(availableModVals)))
        if len(modVals) == 0:
            if self.verbose: print("  ===>> Did not find any new files and force is False")
            return []
        return modVals
    
    def setSliceValues(self, modVals):
        self.sliceValues     = {}
        self.sliceValues["Artist"] = array_split(modVals, min([len(modVals),self.numSlices["Artist"]]))
        self.sliceValues["Media"]  = [self.sliceValues["Artist"] for _ in range(len(self.numSlices["Media"]))]



    #################################################################################################
    #
    # Loading Artist Data
    #
    #################################################################################################
    def loadArtistModValData(self, sliceNum):
        artistMergeTypes  = self.mergeType["Artist"]
        artistSliceValues = self.sliceValues["Artist"][sliceNum]
        artistStr = ", ".join([artistMergeType.inputName for artistMergeType in artistMergeTypes])
        if self.verbose: print(f"   ===> Loading [({artistStr})] ==> ModVal Data ({len(artistSliceValues)})")

        joinStats  = {"Slices": len(artistSliceValues), "Types": [], "End": 0}
        artistModValData = None
        for artistMergeType in artistMergeTypes:
            artistModValFileTypeData = concat([self.dataio.getFileTypeModValData(artistSliceValue, artistMergeType.inputName, force=False) for artistSliceValue in artistSliceValues])
            artistModValData = concat([artistModValData,artistModValFileTypeData]) if isinstance(artistModValData,DataFrame) else artistModValFileTypeData
            joinStats["Types"].append(artistMergeType.inputName)
        
        assert isinstance(artistModValData,DataFrame), f"Artist Data is not a DataFrame [{type(artistModValData)}] in loadArtistModValData"
        joinStats["End"] = artistModValData.shape[0]
        if self.verbose: print(f"      | [{self.showStats(joinStats)}]")
        return artistModValData
    
    

    #################################################################################################
    #
    # Joining Artist Media Data
    #
    #################################################################################################
    def joinArtistMediaModValData(self, artistData, sliceNum):
        def makeMedia(mediaData):
            if not isinstance(mediaData,dict):
                return None
            mediaCollectionData = self.rawio.makeRawMediaCollectionData()
            for mediaTypeName,mediaTypeData in mediaData.items():
                mediaCollectionData.add(mediaTypeName, mediaTypeData)
            return mediaCollectionData
        
        def mergeMedia(artistMediaData, mediaData):
            if not isinstance(mediaData,dict):
                return artistMediaData
            assert self.rawio.isRawMediaCollectionData(artistMediaData)
            mediaDataCollection = makeMedia(mediaData)
            if not self.rawio.isRawMediaCollectionData(mediaDataCollection):
                return artistMediaData
            artistMediaData.merge(mediaDataCollection)
            return artistMediaData
            
        joinStats  = {"Start": artistData.shape, "Slices": 0, "Joins": 0, "Media": 0, "End": 0}
        sliceMergeData = list(zip(self.sliceValues["Media"], self.mergeType["Media"]))
        for (mediaSliceValues,mediaMergeType) in sliceMergeData:
            if isinstance(mediaMergeType.mediaName,str):
                if self.verbose: ts = Timestat(f"Joining [Artist ({mediaMergeType.inputName}) ({mediaMergeType.mediaName}) Media] ==> ModVal Data ({sliceNum})", ind=4)
            else:
                if self.verbose: ts = Timestat(f"   ===> Joining [Artist ({mediaMergeType.inputName}) (All) Media] ==> ModVal Data ({sliceNum})", ind=4)

            #ts = Timestat("Getting Media Data")
            artistMediaData  = concat([self.dataio.getFileTypeModValData(mediaSliceValue, mediaMergeType.inputName, force=False) for mediaSliceValue in mediaSliceValues])
            assert isinstance(artistMediaData,Series), f"Artist MediaType [{artistType.inputName}] is not a Series in joinArtistMediaModValData"

            if "Media" in artistData.columns:
                joinData = {}
                for i,(artistID,artistIDData) in enumerate(artistMediaData[artistMediaData.index.isin(artistData.index)].items()):
                    previousData = artistData.loc[artistID,"Media"]
                    if self.rawio.isRawMediaCollectionData(previousData) and self.rawio.isRawMediaCollectionData(artistIDData):
                        artistData.loc[artistID,"Media"].merge(artistIDData)
                    elif self.rawio.isRawMediaCollectionData(artistIDData):
                        artistData.loc[artistID,"Media"] = artistIDData
            else:
                artistMediaData.name = "Media"
                artistData = artistData.join(artistMediaData)
                    
            if self.verbose: ts.stop()
                    
        joinStats["End"] = artistData.shape
        if self.verbose: print(f"      | [{self.showStats(joinStats)}]")
        return artistData