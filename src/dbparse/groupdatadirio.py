""" I/O For Raw Data """

__all__ = ["RawDataDirIO", "ShuffleDataDirIO", "ShuffleArtistDataDirIO"]

from dbbase import MusicDBRootDataIO
from dbbase import FileSelector, MusicDBIDModVal, getModVals
from utils import FileInfo, DirInfo


class DataDirIOBase:
    def __repr__(self):
        return f"DataDirIOBase(db={self.rdio.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        assert isinstance(rdio, MusicDBRootDataIO), f"rdio [{rdio}] is not a MusicDBRootDataIO"
        self.rdio = rdio
        self.fs = FileSelector(rdio, **kwargs)
        
    def getFileTypeDir(self, fileTypeKey: str, modVal: int) -> 'DirInfo':
        datadir = self.rdio.getDir(fileTypeKey, modVal)
        return datadir
                
    def getDirFiles(self, modVal: int, fileTypeKey, **kwargs) -> 'list':
        assert isinstance(modVal, int), f"modVal [{int}] is not an int"
        verbose = kwargs.get('verbose', self.verbose)
        assert isinstance(verbose, bool), f"force arg [{verbose}] is not a bool"
        force = kwargs.get('force', False)
        assert isinstance(force, bool), f"force arg [{force}] is not a bool"
        expr = kwargs.get('expr', "< 0 Days")
        assert isinstance(expr, str), f"expr arg [{expr}] is not a str"
        tsFile = kwargs.get("tsFile", None)
        assert isinstance(tsFile, FileInfo) or tsFile is None, f"tsFile [{tsFile}] is not FileInfo/None"
        
        if verbose:
            print(f"{self.__repr__()}: (modVal={modVal}, fileTypeKey={fileTypeKey}, force={force}, expr='{expr}')")
        
        datadir = self.getFileTypeDir(fileTypeKey, modVal)
        allFiles = datadir.getFiles()
        newFiles = self.fs.select(allFiles, tsFile, expr, force, verbose=verbose)
        
        if verbose:
            print(f"{self.__repr__()}: Found {len(newFiles)}/{len(allFiles)} New/All Files")
            
        return newFiles
    
    def getDirArtistFiles(self, modVal: int, fileTypeKey, **kwargs) -> 'list':
        assert isinstance(modVal, int), f"modVal [{int}] is not an int"
        verbose = kwargs.get('verbose', self.verbose)
        assert isinstance(verbose, bool), f"force arg [{verbose}] is not a bool"
        force = kwargs.get('force', False)
        assert isinstance(force, bool), f"force arg [{force}] is not a bool"
        expr = kwargs.get('expr', "< 0 Days")
        assert isinstance(expr, str), f"expr arg [{expr}] is not a str"
        tsFile = kwargs.get("tsFile", None)
        assert isinstance(tsFile, FileInfo) or tsFile is None, f"tsFile [{tsFile}] is not FileInfo/None"
        
        if verbose:
            print(f"{self.__repr__()}: (modVal={modVal}, fileTypeKey={fileTypeKey}, force={force}, expr='{expr}')")
        
        allFiles = []
        for globVal in getModVals():
            datadir = self.getFileTypeDir(fileTypeKey, globVal)
            finfo = datadir.join(f"{modVal}-{fileTypeKey}-DB.p")
            if finfo.exists():
                allFiles.append(finfo.path)
        newFiles = self.fs.select(allFiles, tsFile, expr, force, verbose=verbose)
        
        if verbose:
            print(f"{self.__repr__()}: Found {len(newFiles)}/{len(allFiles)} New/All Files")
            
        return newFiles
    

class ShuffleDataDirIO(DataDirIOBase):
    def __repr__(self):
        return f"ShuffleDataDirIO(db={self.rdio.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        super().__init__(rdio, **kwargs)
        
    def getFileTypeKey(self, fileType):
        assert isinstance(fileType, str), "fileType must be a str"
        return f"Shuffle{fileType}" if not fileType.startswith("Shuffle") else fileType
        
    def getNewFiles(self, modVal, fileType, **kwargs) -> 'list':
        fileTypeKey = self.getFileTypeKey(fileType)
        return self.getDirArtistFiles(modVal, fileTypeKey, **kwargs)
     

class ShuffleArtistDataDirIO(DataDirIOBase):
    def __repr__(self):
        return f"ShuffleArtistDataDirIO(db={self.rdio.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        super().__init__(rdio, **kwargs)
        
    def getFileTypeKey(self, fileType):
        assert isinstance(fileType, str), "fileType must be a str"
        return f"Shuffle{fileType}" if not fileType.startswith("Shuffle") else fileType
        
    def getNewFiles(self, modVal, fileType, **kwargs) -> 'list':
        fileTypeKey = self.getFileTypeKey(fileType)
        return self.getDirArtistFiles(modVal, fileTypeKey, **kwargs)
        

class RawDataDirIO(DataDirIOBase):
    def __repr__(self):
        return f"RawDataDirIO(db={self.rdio.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        super().__init__(rdio, **kwargs)
        
    def getFileTypeKey(self, fileType):
        return f"Raw{fileType}ModVal" if isinstance(fileType, str) else "Raw"
        
    def getNewFiles(self, modVal, fileType, **kwargs) -> 'list':
        fileTypeKey = self.getFileTypeKey(fileType)
        return self.getDirFiles(modVal, fileTypeKey, **kwargs)
    
    def getFile(self, dbid: str, fileType: str, **kwargs) -> 'FileInfo':
        fileTypeKey = self.getFileTypeKey(fileType)
        mv = MusicDBIDModVal()
        modVal = mv.getModVal(dbid)
        globVal = mv.getGlobVal(dbid)

        dinfo = self.getFileTypeDir(fileTypeKey, modVal)
        fname = self.getFilename(fileType, modVal, globVal)
        retval = dinfo.join(fname)
        return retval

    def getFilename(self, fileType: str, modVal, globVal, **kwargs) -> 'str':
        mv = MusicDBIDModVal()
        fModVal = mv.getModGlobVal(modVal)
        fGlobVal = mv.getModGlobVal(globVal)
        finfo = f"{self.rdio.db}-{fileType}-mv-{fModVal}-gv-{fGlobVal}.p"
        return finfo
        