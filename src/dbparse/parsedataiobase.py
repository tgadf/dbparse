""" Base IO Class For Parsing Data """

__all__ = ["ParseDataIOBase"]

from dbbase import MusicDBIDModVal, MusicDBRootDataIO
from dbraw import RawDataIOBase
from .parsedatafileio import ParseDataFileIO
from .modvaldataoutput import ModValDataOutput


class ParseDataIOBase:
    def __repr__(self):
        return f"ParseDataIOBase(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, rawio: RawDataIOBase, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.mvdopt = ModValDataOutput(rdio, **kwargs)
        self.pdfio = ParseDataFileIO(rdio, rawio, **kwargs)
        self.rddio = self.pdfio.rddio
        self.dataio = self.pdfio.dataio
        self.db = rdio.db
        self.mv = MusicDBIDModVal()
        self.procs = {}
        
    ###########################################################################
    # Easy I/O
    ###########################################################################
    def isUpdateModVal(self, n) -> 'bool':
        if self.verbose is False:
            return False
        retval = True if ((n + 1) % 25 == 0 or (n + 1) == 5) else False
        return retval
    
    ###########################################################################
    # Parse Runner
    ###########################################################################
    def parse(self, modVal=None, key=None, **kwargs) -> 'list':
        if key in self.procs.keys():
            self.procs[key].parse(modVal=modVal, **kwargs)
        else:
            for key, proc in self.procs.items():
                proc.parse(modVal=modVal, **kwargs)
                
    ###########################################################################
    # Helper Base Functions
    ###########################################################################
    def getRawParser(self, parseType):
        rawParser = getattr(self.pdfio.rawio, parseType.parseFuncName)
        assert callable(rawParser), f"There is no [{rawParser}] function to parse the raw data"
        return rawParser
    
    def getRawFiles(self, modVal, parseType, force=False, verbose=False) -> 'list':
        inputNames = parseType.inputName if isinstance(parseType.inputName, list) else [parseType.inputName]
        tsFile = self.dataio.mvdataio.getFile(modVal)
        newFiles = []
        for inputName in inputNames:
            newFiles += self.rddio.getNewFiles(modVal=modVal, fileType=inputName,
                                               tsFile=tsFile, force=force, verbose=verbose)
        return newFiles
    
    