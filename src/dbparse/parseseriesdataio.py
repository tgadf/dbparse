""" Base Class For Parsing Series ModVal Data """__all__ = ["ParseSeriesDataIO"]from dbbase import MusicDBRootDataIOfrom utils import Timestatfrom pandas import Seriesfrom .parsedataiobase import ParseDataIOBasefrom .parsestats import ParseStatsfrom .parsefiletype import ParseFileType################################################################################ Series Parser###############################################################################class ParseSeriesDataIO(ParseDataIOBase):    def __repr__(self):        return f"ParseSeriesDataIO(db={self.dataio.db})"            def __init__(self, rdio: MusicDBRootDataIO, rawio, parseType: ParseFileType, **kwargs):        super().__init__(rdio, rawio, **kwargs)        self.parseType = parseType                if self.verbose:            print(f"  {'ParseSeriesDataIO:': <25} [{parseType.inputName}] => [{parseType.outputName}]")        def parse(self, inputSeries, **kwargs):        verbose = kwargs.get('verbose', self.verbose)        force = kwargs.get('force', False)        test = kwargs.get('test', False)        force = True if test is True else force        ts = Timestat(f"Parsing {self.db} [Raw Series] => [ModVal {self.parseType.outputName}]", verbose=verbose)                    assert isinstance(inputSeries, Series), "Input is not a Series in SeriesParse()"        rawParser = self.getRawParser(self.parseType)            inputSeries.index = inputSeries.index.map(lambda artistID: (artistID, self.mv.getModVal(artistID)))        for n, (modVal, modValGroupData) in enumerate(inputSeries.groupby(axis=0, level=1)):            if self.isUpdateModVal(n):                ts.update(n=n + 1)            if modValGroupData.shape[0] == 0:                continue            modValGroupData = modValGroupData.droplevel(1)                parseStats = {}            self.pdfio.getInput(modVal, self.parseType, parseStats=parseStats, force=force)            parseStats.update({"Good": 0, "Bad": 0})            for fid, fdata in modValGroupData.iteritems():                rData = rawParser(fid, fdata, ifile=None)                status = self.pdfio.updateModValData(rData, self.parseType)                if status is True:                    parseStats["Good"] += 1                else:                    parseStats["Bad"] += 1                if test is True:                print("Stopping early due to test == True")                break                                self.pdfio.getEndStats(parseStats)            if parseStats["Good"] == 0:                print(f"   ===> Not Saving {self.parseType.outputName} ModVal={modVal} Data.")                continue                    ##############################            # Save Output            ##############################            self.pdfio.saveOutput(modVal, self.parseType, parseStats, force=force)                    ts.stop()