from dbmaster import MasterDBs
from dbbase import MusicDBRootDataIO
from dbparse import ModValDataOutput

    
def test_modvaloutput():
    dbs = MasterDBs().getDBs()
    rdio = MusicDBRootDataIO(dbs[0])

    mvdo = ModValDataOutput(rdio)
    assert hasattr(mvdo, 'save'), f"mvdo [{mvdo}] does not have a save function"


if __name__ == "__main__":
    test_modvaloutput()
    