from dbmaster import MasterDBs
from dbbase import MusicDBRootDataIO, MusicDBParamsBase
from dbparse import MusicDBGroupDataIO


# This test does very little. Needs to be in the musicdb.*db area
def test_groupdata():
    dbs = MasterDBs().getDBs()
    rdio = MusicDBRootDataIO(dbs[0])    
    params = MusicDBParamsBase()
    gdio = MusicDBGroupDataIO(rdio=rdio, params=params)
    assert gdio.db == rdio.db, f"gdio db [{gdio.db}] is not correct"


if __name__ == "__main__":
    test_groupdata()
    