#!/usr/bin/env python

# +-+-+-+-+-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |O|R|A|C|L|E| |C|H|E|C|K|E|R| |I|M|P|L|E|M|E|N|T|A|T|I|O|N|
# +-+-+-+-+-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+-+-+-+-+

import logging
import cx_Oracle
import commands
import sys
import os
from datetime import datetime

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

# - Static definitions

#STATIC_DESTINATION_ORIGINAL="SELECT name, ROUND(SPACE_LIMIT/1024/1024,2) TOTAL, ROUND(SPACE_USED/1024/1024,2) USADO, NUMBER_OF_FILES ,ROUND((SPACE_USED/SPACE_LIMIT)*100,2) \'%PERCENT_USED\' from v$recovery_file_dest;"
STATIC_DESTINATION="SELECT ROUND((SPACE_USED/SPACE_LIMIT)*100,2) FROM v$recovery_file_dest"
PMON_CHECKER="ps -fea | grep pmon | grep -v grep | awk ' { print $8 } '"
PMON_VALUE="ora_pmon_EMO"
ORACLE_VARS=[("ORACLE_BASE","/home/oracle/app/oracle/product/11.2.0"),("ORACLE_HOME","/home/oracle/app/oracle/product/11.2.0/dbhome_1"),("ORACLE_SID","EMO")]
#THRESHOLDS=[("CRIT",70),("WARN",65)]
THRESHOLDS=[("CRIT",25),("WARN",10)]

# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+
# |C|L|A|S|S| |D|E|F|I|N|I|T|I|O|N|
# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+

class OracleFacade:
    
    connection = None
    myCursor = None
    myQueryResult = []
    myCleanValue = None
    myMode = cx_Oracle.SYSDBA
    commandStatus = 0
    commandResult = ''
    
    def openConnection(self,cls):
        try:
            self.connection = cx_Oracle.Connection(mode = self.myMode)
            self.myCursor = self.connection.cursor()
        except Exception as err:
            cls.exception("Exception occurred during database connection.")

    def closeConnection(self):
        try:
            self.connection.close()
        except Exception as err:
            cls.exception("Exception occurred closing the DB session.")

    def query(self,QUERY,flag):
        self.myCursor.execute(QUERY)
        self.myQueryResult = self.myCursor.fetchall()
        
        if flag == 0:
            # - Clean Result
            for i in self.myQueryResult:
                self.myCleanValue = i[0]
        else:
            #- Gather stats instead
            print("Will be Gather Stats instead!")

    def pmStatus(self):
        self.commandStatus, self.commandResult = commands.getstatusoutput(PMON_CHECKER)

    def exportOracleVars(self,variables):
        for i in variables:
            variableName, variableValue = i
            os.environ[variableName] = variableValue

    @staticmethod
    def elegantExit():
        sys.exit()

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+   

class NagiosNotification:
    
    criticalThreshold = 0
    warningThreshold = 0
    resultStatus = 0 
    destinationFinalValue = None
    statusList = [("CRITICAL",2),("WARNING",1),("NORMAL OK",0)]

    # - THRESHOLDS=[("CRIT",70),("WARN",65)]  
    def deployThresholds(self,myThresholds):
        for i in myThresholds:
            myTempString, myTempValue = i
            if myTempString == "CRIT":
                self.criticalThreshold = myTempValue
            else:
                self.warningThreshold = myTempValue
 
    def getStatusDestinationFile(self,currentValue):
        self.destinationFinalValue = currentValue
        if currentValue < self.warningThreshold:
            self.resultStatus = 0
        elif ( currentValue > self.warningThreshold ) and ( currentValue < self.criticalThreshold ):
            self.resultStatus = 1
        elif currentValue >= self.criticalThreshold:
            self.resultStatus = 2

    def returnValueToNagios(self,flag):
        if flag == 0:
            # - Return to nagios
            # - Original string | $status chk_Dest_File - USAGE $SP%  at $TIME | TOTAL $TOTAL USADO $USADO | FILES $FILES
            myTimeStamp = str(datetime.now())
            destinationFinalValueString = str(self.destinationFinalValue)
            resultStatusString = str(self.resultStatus)
            specialChar = "%"
            for x in self.statusList:
                label, value = x
                if value == self.resultStatus:
                    finalLabel = label
            myTempString = '%s chk_Dest_File %s | Usage: %s%s at %s' % (resultStatusString, finalLabel, destinationFinalValueString, specialChar, myTimeStamp)             
            print(myTempString)
        else:
            #- Gather stats instead
            print("Will be Gather Stats instead!")

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class QueryBuilder:

    query = ""
    flag = 0

    def prepareQuery(self):
        if self.flag == 0:
            # - Destination query
            self.query = STATIC_DESTINATION

	elif selfflag == 1:
            # - Statistics query
            self.query = STATIC_DESTINATION

    def destinationChecker(self):
        self.flag = 0

    def gatherStats(self):
        self.flag = 1

# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+
# |D|E|P|L|O|Y|M|E|N|T| |L|O|G|I|C|
# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+

# - Logger setup

botLogger = logging.getLogger(__name__)
f_handler = logging.FileHandler('checker_test.log')
f_handler.setLevel(logging.ERROR)
f_format = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
f_handler.setFormatter(f_format)
botLogger.addHandler(f_handler)

# - Objects

myOracleQB = QueryBuilder()
myOracleFacade = OracleFacade()
myNagiosBox = NagiosNotification()

# - Set checker needed

myOracleQB.destinationChecker()
myOracleQB.prepareQuery()
myOracleFacade.pmStatus()

if( myOracleFacade.commandStatus == 0 ) and ( myOracleFacade.commandResult == PMON_VALUE ):
    # - PMON Exist
    myOracleFacade.exportOracleVars(ORACLE_VARS)
    myOracleFacade.openConnection(botLogger)

    # - Query DB
    myOracleFacade.query(myOracleQB.query,myOracleQB.flag)
    
    # - Close connection
    myOracleFacade.closeConnection()

    # - Deploy thresholds
    myNagiosBox.deployThresholds(THRESHOLDS)

    # - Get status
    myNagiosBox.getStatusDestinationFile(myOracleFacade.myCleanValue)

    # - Notify to Nagios and terminate
    myNagiosBox.returnValueToNagios(myOracleQB.flag)
    #print(NagiosNotification.resultStatus)
    #print(myOracleFacade.myCleanValue)

else:
    # - PMON does not exist, elegant exit called
    OracleFacade.elegantExit()

