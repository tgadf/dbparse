from dbmaster import MasterDBs
from dbbase import MusicDBRootDataIO
from dbparse import RawDataDirIO


def test_rawdataio():
    dbs = MasterDBs().getDBs()
    rdio = MusicDBRootDataIO(dbs[0])
    rddio = RawDataDirIO(rdio)

    assert hasattr(rddio, "getNewFiles"), f"getNewFiles function not found in {rddio}"
    assert callable(getattr(rddio, "getNewFiles")), f"getNewFiles function is not callable in {rddio}"
    
    assert hasattr(rddio, "getFile"), f"getNewFile function not found in {rddio}"
    assert callable(getattr(rddio, "getFile")), f"getFile function is not callable in {rddio}"
