import sys
import pprint
sys.path.append('../library')
from gdaScore import gdaAttack
from logging.handlers import TimedRotatingFileHandler
import  logging

pp = pprint.PrettyPrinter(indent=4)
_Log_File="../log/utility.log"

def createTimedRotatingLog():
    logger =logging.getLogger('RotatingLog')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s| %(levelname)s| %(message)s','%m/%d/%Y %I:%M:%S %p')
    handler = TimedRotatingFileHandler(_Log_File,when='midnight',interval=1,backupCount=0)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logging = createTimedRotatingLog()

class gdaUtility:
    _allresults={}
    _nocoldiff=0
    _accuracy=1.0000
    _coverage=1.0000
    def __init__(self,accuracy,coverage):
        self.accuracy=accuracy
        self.coverage=coverage
        gdaUtility._nocoldiff=gdaUtility._nocoldiff+1
        gdaUtility._allresults.update({str(gdaUtility._nocoldiff):{self.coverage,self.accuracy}})


    #Method to get tables and columns from rawDb and anonDb
    def _getTableAndColumns(self,params):
        try:
            x=gdaAttack(params)
            rawDbdict=dict()
            anonDbdict=dict()
            rawdbType='rawDb'
            anondbType='anonDb'
            #get table and columns of rawDb
            tables = x.getTableNames(dbType=rawdbType)
            for table in tables:
                cols = x.getColNamesAndTypes(dbType=rawdbType,tableName=table)
                rawDbdict[table]=cols
            #get table and columns of anonDb
            tables = x.getTableNames(dbType=anondbType)
            for table in tables:
                cols = x.getColNamesAndTypes(dbType=anondbType, tableName=table)
                anonDbdict[table] = cols
            logging.info('RawDb Dictionary and AnnonDb Dictionary: %s and %s',rawDbdict,anonDbdict)
            x.cleanUp()
        except:
            logging.error('Error occured while connecting dB or cleaning up the used resources: anonDBStatus %s : rawDbStatus %s',anonDbdict,rawDbdict)
        return rawDbdict,anonDbdict

    #Method to check columns present in rawDb, but not in anonDb
    def _checkExtraColumninrawDb(self,rawDbDictionary,anonDbDictionary):
        _noColumns=0;
        _excludedColumnsList={}
        _columnPosition=0;
        _columnTypePosition=1;
        logging.info('Inside the method:_checkExtraColumninrawDb')
        for table in rawDbDictionary:
            _excludedColumnsList[table]={}
            rawDbList=rawDbDictionary[table]
            #construct anonDb column list to check rawDb columns are present.
            anonDbList=anonDbDictionary[table]
            anonDbColumns=[]
            for anonDbColumnDesc in anonDbList:
                anonDbColumns.append(anonDbColumnDesc[_columnPosition])
            #check if anonDb contains all the columns that are in rawDb, if not increment the _noColumns
            for rawDbColumnDesc in rawDbList:
                if not rawDbColumnDesc[_columnPosition] in anonDbColumns:
                    _noColumns=_noColumns+1
                    _excludedColumnsList[table][rawDbColumnDesc[_columnPosition]]=rawDbColumnDesc[_columnTypePosition]
            logging.info('Number of columns that are not excluded for query: %s and the completeList: %s',_noColumns,_excludedColumnsList)
        return _noColumns,_excludedColumnsList


    #Method to check generate histogram
    def _generateHistogram(self,params,rawdb,anondb,clientId='client_id'):
        x=gdaAttack(params)
        _noOftry=1
        _columnPosition = 0;
        _coverage={}
        _accuracyAbsolute={}
        _accuracyRelative={}
        _noQueries=0
        try:
            for table in rawdb:
                _coverage[table] = {}
                _accuracyAbsolute[table] = {}
                _accuracyRelative[table] = {}
                for columnDesc in rawdb[table]:
                    columnName=columnDesc[_columnPosition]
                    _coverage[table][columnName]=[]
                    _accuracyAbsolute[table][columnName]=[]
                    _accuracyRelative[table][columnName]=[]
                    sql=str(f"SELECT {columnName}, count(distinct {clientId}) FROM {table} GROUP BY 1")
                    logging.info('RawDb-Query: %s',sql)
                    _noQueries=_noQueries+1
                    query = dict(db="raw", sql=sql)
                    answer = self._queryDb(_noOftry, sql, x,query)
                    rawDbrows=answer['answer']
                    rawDbrowsDict={}
                    logging.info('query-Answer: RowSize: %s',len(rawDbrows))
                    for row in rawDbrows:
                        rawDbrowsDict[row[0]]=row[1]
                    if len(rawDbrows) > 1:
                        query = dict(db="anon", sql=sql)
                        sql = str(f"SELECT {columnName}, count(distinct {clientId}) FROM {table} GROUP BY 1")
                        logging.info('AnonDb-Query: %s', sql)
                        #print(f"sql is {sql}")
                        answer = self._queryDb(_noOftry, sql, x,query)
                        anonDbrows = answer['answer']
                        anonDbrowsDict = {}
                        logging.info('query-Answer: RowSize: %s', len(anonDbrows))
                        for row in anonDbrows:
                            anonDbrowsDict[row[0]] = row[1]
                        _excluderows=0;
                        for key in  anonDbrowsDict:
                            if key  in rawDbrowsDict.keys():
                                _accuracyAbsolute[table][columnName].append(str(abs(anonDbrowsDict[key]-rawDbrowsDict[key])))
                                _accuracyRelative[table][columnName].append(str((abs(anonDbrowsDict[key]-rawDbrowsDict[key]))/(max(anonDbrowsDict[key],rawDbrowsDict[key]))))
                                _excluderows=_excluderows+1
                        _coverage[table][columnName]=str(1-((abs(len(rawDbrowsDict)-_excluderows))/len(rawDbrowsDict)))
                        #print(f"Size of rawDbRows: {len(rawDbrows)} and anonDbRows: {len(anonDbrows)}")
                    elif (len(rawDbrows))==1:
                        _coverage[table][columnName] = None

            x.cleanUp()
        except:
            logging.error('Error occured while querying or while cleaning up the used resources')
        return _accuracyAbsolute,_coverage,_noQueries,_accuracyRelative;


    def _generateHistogramForTwoColumns(self,params,rawdb,anondb,columnPairs=2,client_id='client_id'):
        #print(f"Number of column Pairs for query: {columnPairs}")
        x = gdaAttack(params)
        _noOftry = 1
        _columnPosition = 0;
        _coverage = {}
        _accuracyAbsolute = {}
        _accuracyRelative = {}
        _noQueries = 0
        try:
            for table in rawdb:
                _coverage[table] = {}
                _accuracyAbsolute[table] = {}
                _accuracyRelative[table] = {}
                for i in range (0,len(rawdb[table])):
                    j=i+1
                    column1=rawdb[table][i][0]
                    for j in range (j,len(rawdb[table])):
                        column2 = rawdb[table][j][0]
                        _accuracyAbsolute[table][''+str(i)+str(j)] = []
                        _accuracyRelative[table][''+str(i)+str(j)] = []
                        sql = str(f"SELECT {column1},{column2}, count(distinct {client_id}) FROM {table} GROUP BY 1,2")
                        logging.info('MultiCol:RawDb-Query: %s', sql)
                        _noQueries = _noQueries + 1
                        query = dict(db="raw", sql=sql)
                        answer = self._queryDb(_noOftry, sql, x, query)
                        #print(f" answer: {answer}")
                        rawDbrows = answer['answer']
                        rawDbrowsDict = {}
                        logging.info('MultiCol:query-Answer: RowSize', len(rawDbrows))
                        for row in rawDbrows:
                            rawDbrowsDict[''+str(row[0])+str(row[1])] = row[2]
                        if len(rawDbrows) > 1:
                            query = dict(db="anon", sql=sql)
                            sql = str(f"SELECT {column1},{column2}, count(distinct {client_id}) FROM {table} GROUP BY 1,2")
                            logging.info('MultiCol:Anon-Query: %s', sql)
                            answer = self._queryDb(_noOftry, sql, x, query)
                            anonDbrows = answer['answer']
                            anonDbrowsDict = {}
                            logging.info('MultiCol:query-Answer: RowSize %s', len(anonDbrows))
                            for row in anonDbrows:
                                anonDbrowsDict[''+str(row[0])+str(row[1])] = row[2]
                            _excluderows = 0;
                            for key in anonDbrowsDict:
                                if key in rawDbrowsDict.keys():
                                    _accuracyAbsolute[table][''+str(i)+str(j)].append(
                                        str(abs(anonDbrowsDict[key] - rawDbrowsDict[key])))
                                    _accuracyRelative[table][''+str(i)+str(j)].append(str(
                                        (abs(anonDbrowsDict[key] - rawDbrowsDict[key])) / (
                                            max(anonDbrowsDict[key], rawDbrowsDict[key]))))
                                    _excluderows = _excluderows + 1
                            _coverage[table][''+str(i)+str(j)] = str(
                                1 - ((abs(len(rawDbrowsDict) - _excluderows)) / len(rawDbrowsDict)))
                            # print(f"Size of rawDbRows: {len(rawDbrows)} and anonDbRows: {len(anonDbrows)}")
                        elif (len(rawDbrows)) == 1:
                            _coverage[table][''+str(i)+str(j)] = None

            x.cleanUp()
        except:
            logging.error('Error occured while querying or while cleaning up the used resources')
        return _accuracyAbsolute, _coverage, _noQueries, _accuracyRelative;

    #Method to query the database and returns the fetched tuples.
    def _queryDb(self, _noOftry, sql, x,query):
        for i in range(_noOftry):
            query['myTag'] = i
            x.askExplore(query)
        while True:
            answer = x.getExplore()
            # print(answer)
            tag = answer['query']['myTag']
            # print(f"myTag is {tag}")
            if answer['stillToCome'] == 0:
                break
        return answer


#rawdb,anondb=gdaUtility._getTableAndColumns('gdaScoreBankingRaw','cloakBankingAnon')



obj1=gdaUtility(accuracy=1,coverage=1)
params = dict(name='utilitygdaUtility',rawDb='gdaScoreBankingRaw',anonDb='cloakBankingAnon',table='accounts',criteria='singlingOut',flushCache=True,verbose=False)
rawdb,anondb=obj1._getTableAndColumns(params)
noColumns,columnsOnlyInrawDb=obj1._checkExtraColumninrawDb(rawdb,anondb)
#print (f"rawdb {rawdb}")
primary_key='client_id'
accuracy,coverage,noQueries,accuracyRelative=obj1._generateHistogram(params,rawdb,anondb,primary_key)

print(f"overall coverage calculation:")
sum=0
for key in coverage:
    for columnk in coverage[key]:
     #print(f"column {columnk}")
     if coverage[key][columnk] is not None:
         sum = sum + float(coverage[key][columnk])
logging.info('Number of queries:%s ',noQueries)
print(f"Total Coverage for Single Column:{(sum/noQueries)}")


def meanSquareError(accuracy,value='absol'):
    accuracyAbsPerCol = {}
    for tableAcc in accuracy:
        accuracyAbsPerCol[tableAcc] = {}
        for columnName in accuracy[tableAcc]:
            sum = 0
            j = 0;
            acc_score = 0
            if (accuracy[tableAcc][columnName] is not None and len(accuracy[tableAcc][columnName]) >= 1):
                for value in accuracy[tableAcc][columnName]:
                    if value == 'absol':
                        sum = sum + int(value)
                    else:
                        sum = sum + float(value)
                    j = j + 1
                if (j != 0):
                    acc_score = sum / j
                    accuracyAbsPerCol[tableAcc][columnName] = str(acc_score)
    return accuracyAbsPerCol

accuracyAbsPerCol=meanSquareError(accuracy)
accuracyRelativePerCol=meanSquareError(accuracyRelative,'relative')


#for key in accuracy:
#    print(f"key {key} and value : {accuracy[key]}")
#print(f"raw db {rawdb}")
#print(f"anon db {anondb}")


print(f"Mean Absolute Error Per Column: {accuracyAbsPerCol}")
print(f"Mean Absolute Error Per Column: {accuracyRelativePerCol}")
'''
accuracyValltiColumn,coverageMultiColumn,noQueriesMultiCol,accuracyValueRelativeMulticol=obj1._generateHistogramForTwoColumns(params,rawdb,anondb,primary_key)

print(f"------------------------------------------------")
print(f"MultiColumn------------------------------------------------")
print(f" no of queries : {noQueriesMultiCol}")
print(f"overall coverage calculation:")
sum=0
for key in coverageMultiColumn:
    for columnk in coverageMultiColumn[key]:
     #print(f"column {columnk}")
     if coverageMultiColumn[key][columnk] is not None:
         sum = sum + float(coverageMultiColumn[key][columnk])
logging.info('Number of queries MultiCol:%s ',noQueriesMultiCol)
print(f"Total Coverage for MultiCol Column:{(sum/noQueriesMultiCol)}")
accuracyMultiCol=meanSquareError(accuracyValltiColumn)
accuracyRelativeMultiCol=meanSquareError(accuracyValueRelativeMulticol,'relative')
print(f"Mean Absolute Error Per Column: {accuracyMultiCol}")
print(f"Mean Absolute Error Per Column: {accuracyRelativeMultiCol}")

'''