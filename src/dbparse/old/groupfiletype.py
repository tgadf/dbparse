""" Basic IO Definitions For Grouping """

__all__ = ["GroupFileType"]

from types import MethodType

class GroupFileType:
    def __repr__(self):
        return f"GroupFileType(inputName={self.inputName}, inputType={self.inputType}, mediaType={self.mediaType}, outputName={self.outputName}, outputLevel={self.outputLevel}, outputFormat={self.outputFormat})"
    
    def __init__(self, inputName, inputType, mediaType, outputName, outputLevel, outputFormat):
        assert isinstance(inputName,(list,str,MethodType)) or inputName is None, "InputFileType must be a list,string or None"
        self.inputName   = inputName
        assert isinstance(inputType,str), "inputType must be a string"
        self.inputType   = inputType
        assert isinstance(mediaType,str), "InputMediaType must be a string"
        self.mediaType   = mediaType
        assert isinstance(outputName,(list,str)) or outputName is None, "OutputFileType must be None or (list,str)"
        self.outputName  = outputName
        assert isinstance(outputLevel,(list,int)), "OutputFileLevel must be None or (list,int)"
        self.outputLevel = outputLevel
        assert isinstance(outputFormat,(list,str)) or outputFormat is None, "Output format must be a string or None"
        self.outputFormat = outputFormat
        if isinstance(outputFormat,list):
            for value in outputFormat:
                assert value in [None, "DataFrame", "Series"], "OutputFormat must be either [None, DataFrame, Series]"
        elif isinstance(outputFormat,str):
            assert outputFormat in [None, "DataFrame", "Series"], "OutputFormat must be either [None, DataFrame, Series]"
        if isinstance(outputName,list):
            assert isinstance(outputLevel,list), "OutputFileList must be a list since OutputFileType is a list"
            for value in outputLevel:
                assert isinstance(value,int), "outputLevel must be an int"
            

    def getOutput(self, level=None):
        if isinstance(self.outputName,list):
            output = dict(zip(self.outputName, list(zip(self.outputLevel,self.outputFormat))))
        else:
            output = {self.outputName: [self.outputLevel,self.outputFormat]}
        retval = {k: v for k,v in output.items() if v[0] == level} if isinstance(level,int) else output
        return retval
    
    def getOutputGroupType(self, outputName, outputInfo):
        return GroupFileType(inputName=None, by=None, outputName=outputName, outputLevel=outputInfo[0], outputFormat=outputInfo[1])
        
    def get(self):
        return self.__dict__