""" Base Class For Parsing DataFrame ModVal Data """__all__ = ["ParseDataFrameDataIO"]from dbbase import MusicDBRootDataIOfrom utils import Timestatfrom pandas import DataFramefrom .parsedataiobase import ParseDataIOBasefrom .parsestats import ParseStatsfrom .parsefiletype import ParseFileType################################################################################ DataFrame Parser###############################################################################class ParseDataFrameDataIO(ParseDataIOBase):    def __repr__(self):        return f"ParseDataFrameDataIO(db={self.dataio.db})"            def __init__(self, rdio: MusicDBRootDataIO, rawio, parseType: ParseFileType, **kwargs):        super().__init__(rdio, rawio, **kwargs)        self.parseType = parseType                if self.verbose:            print(f"  {'ParseDataFrameDataIO:': <25} [{parseType.inputName}] => [{parseType.outputName}]")        def parse(self, inputDataFrame, **kwargs):        verbose = kwargs.get('verbose', self.verbose)        force = kwargs.get('force', False)        test = kwargs.get('test', False)        force = True if test is True else force        ts = Timestat(f"Parsing {self.db} [Raw DataFrame] => [ModVal {self.parseType.outputName}]", verbose=verbose)                    assert isinstance(inputDataFrame, DataFrame), "Input is not a DataFrame in dataframeParse()"        assert "ModVal" in inputDataFrame.columns, "Input DataFrame does not have a ModVal column"        rawParser = eval(f"self.rawio.get{self.parseType.parseName}Data")        assert callable(rawParser), f"There is no [{rawParser}] function to parse the raw data"        for n, (modVal, modValGroupData) in enumerate(inputDataFrame.groupby("ModVal")):            if self.isUpdateModVal(n):                ts.update(n=n + 1)            parseStats = {}            self.pdfio.getInput(modVal, self.parseType, parseStats=parseStats, force=force)            parseStats.update({"Good": 0, "Bad": 0})            for fid, fdata in modValGroupData.iterrows():                rData = rawParser(fid, fdata, ifile=None)                status = self.pdfio.updateModValData(rData, self.parseType)                if status is True:                    parseStats["Good"] += 1                else:                    parseStats["Bad"] += 1                                self.pdfio.getEndStats(parseStats)            if parseStats["Good"] == 0:                print(f"   ===> Not Saving {self.parseType.outputName} ModVal={modVal} Data.")                continue                    ##############################            # Save Output            ##############################            self.pdfio.saveOutput(modVal, self.parseType, parseStats, force=force)            if test is True:                print("Stopping early due to test == True")                break                    ts.stop()