""" Basic IO Definitions For Merging """

__all__ = ["MergeFileType"]


class MergeFileType:
    def __repr__(self):
        return f"MergeFileType(mediaName={self.mediaName}, inputName={self.inputName}, inputType={self.inputType}, outputName={self.outputName}, outputFormat={self.outputFormat})"
            
    def __init__(self, inputName, inputType, outputName, outputFormat, mediaName=None):
        assert isinstance(mediaName, str) or mediaName is None, "MediaName must be a string or None"
        self.mediaName = mediaName
        
        assert isinstance(inputName, (list, str)) or inputName is None, "InputName must be a string,list or None"
        self.inputName = inputName
        
        assert isinstance(inputType, str), "InputType must be a string or None"
        assert inputType in ["Artist", "ArtistConcat", "Media", "Profile", "Shuffle", "ShuffleArtist"], "InputType must be either [Artist, ArtistConcat, Media, Profile, Shuffle, ShuffleArtist]"
        self.inputType = inputType
        self.mergeClassName = f"Merge{self.inputType}DataIO"
        
        assert isinstance(outputName, str) or outputName is None, "OutputName must be a string or None"
        self.outputName = outputName
        
        assert isinstance(outputFormat, str) or outputFormat is None, "Output format must be a string or None"
        assert outputFormat in [None, "DataFrame", "Series"], "OutputFormat must be either [None, DataFrame, Series]"
        self.outputFormat = outputFormat
        
    def getOutput(self):
        if isinstance(self.outputName, str):
            output = {self.outputName: [1, self.outputFormat]}
        elif self.outputName is None:
            output = {self.outputName: [0, self.outputFormat]}
        else:
            raise ValueError(f"OutputName [{self.outputName}] is not allowed in getOutput()")
        return output
                             
    def getOutputMergeType(self, outputName, outputFormat):
        return MergeFileType(mediaName=None, inputName=None, inputType=None, outputName=outputName, outputFormat=outputFormat)
                          
    def get(self):
        return self.__dict__