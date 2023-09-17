""" Basic IO Definitions For Parsing """

__all__ = ["ParseFileType"]

from types import MethodType


class ParseFileType:
    def __repr__(self):
        return f"ParseFileType(inputName={self.inputName}, parseName={self.parseName}, inputType={self.inputType}, outputName={self.outputName}, outputLevel={self.outputLevel}, outputFormat={self.outputFormat})"
    
    def __init__(self, inputName, parseName, inputType, outputName, outputLevel, outputFormat):
        assert isinstance(inputName, (list, str, MethodType)) or inputName is None, "InputFileType must be a list,string or None"
        self.inputName = inputName
        
        assert isinstance(parseName, str) or parseName is None, "parseName [{parseName}] must be a string or None"
        self.parseFuncName = f"get{parseName}Data"
        
        self.parseName = parseName
        assert isinstance(outputName, (list, str)) or outputName is None, "OutputFileType must be None or (list,str)"
        
        self.inputType = inputType
        assert isinstance(inputType, str), "inputType must be str"
        self.parseClassName = f"Parse{self.inputType}DataIO"
        
        self.outputName = outputName
        assert isinstance(outputLevel, (list, int)), "OutputFileLevel must be None or (list,int)"
        
        self.outputLevel = outputLevel
        assert isinstance(outputFormat, (list, str)) or outputFormat is None, "Output format must be a string or None"
        
        self.outputFormat = outputFormat
        if isinstance(outputFormat, list):
            for value in outputFormat:
                assert value in [None, "DataFrame", "Series"], "OutputFormat must be either [None, DataFrame, Series]"
        elif isinstance(outputFormat, str):
            assert outputFormat in [None, "DataFrame", "Series"], "OutputFormat must be either [None, DataFrame, Series]"
        if isinstance(outputName, list):
            assert isinstance(outputLevel, list), "OutputFileList must be a list since OutputFileType is a list"
            for value in outputLevel:
                assert isinstance(value, int), "outputLevel must be an int"

    def getOutput(self, level=None):
        if isinstance(self.outputName, list):
            output = dict(zip(self.outputName, list(zip(self.outputLevel, self.outputFormat))))
        else:
            output = {self.outputName: [self.outputLevel, self.outputFormat]}
        retval = {k: v for k, v in output.items() if v[0] == level} if isinstance(level, int) else output
        return retval
    
    def getOutputParseType(self, outputName, outputInfo):
        return ParseFileType(inputName=None, parseName=None, outputName=outputName, outputLevel=outputInfo[0], outputFormat=outputInfo[1])
        
    def get(self):
        return self.__dict__