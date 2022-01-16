from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "CREATE TABLE Query(queryID INTEGER NOT NULL ,"
                     "purpose TEXT NOT NULL,"
                     "size INTEGER NOT NULL,"
                     "UNIQUE(queryID),"
                     "CHECK(queryID>0),"
                     "CHECK(size >= 0));"
                     "CREATE TABLE Disk(diskID INTEGER NOT NULL ,"
                     "company TEXT NOT NULL,"
                     "speed INTEGER NOT NULL,"
                     "free_space INTEGER NOT NULL,"
                     "cost INTEGER NOT NULL,"
                     "UNIQUE(diskID),"
                     "CHECK(diskID>0),"
                     "CHECK(speed>0),"
                     "CHECK(cost>0),"
                     "CHECK(free_space>=0));"
                     "CREATE TABLE Ram(ramID INTEGER NOT NULL,"
                     "company TEXT NOT NULL,"
                     "size INTEGER NOT NULL ,"
                     "UNIQUE(ramID),"
                     "CHECK(ramID>0),"
                     "CHECK(size>0));"
                     "CREATE TABLE DiskandRam(diskID INTEGER,"
                     "ramID INTEGER,"
                     "FOREIGN KEY(diskID) REFERENCES Disk(diskID) ON DELETE CASCADE,"
                     "FOREIGN KEY(ramID) REFERENCES Ram(ramID) ON DELETE CASCADE,"
                     "PRIMARY KEY (diskID,ramID));"
                     "CREATE TABLE DiskandQuery(diskID INTEGER,"
                     "queryID INTEGER,"
                     "queryPurpose TEXT NOT NULL,"
                     "querySize INTEGER NOT NULL ,"
                     "FOREIGN KEY(diskID) REFERENCES Disk(diskID) ON DELETE CASCADE,"
                     "FOREIGN KEY(queryID) REFERENCES Query(queryID) ON DELETE CASCADE,"
                     "PRIMARY KEY (diskID,queryID));"
                     "COMMIT;")
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
    except Exception as e:
        conn.rollback()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM Query;"
                     "DELETE FROM Disk;"
                     "DELETE FROM Ram;"
                     "DELETE FROM DiskandQuery;"
                     "DELETE FROM DiskandRam;"
                     "COMMIT;")
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS Query CASCADE;"
                     "DROP TABLE IF EXISTS Disk CASCADE;"
                     "DROP TABLE IF EXISTS Ram CASCADE;"
                     "DROP TABLE IF EXISTS DiskandQuery CASCADE;"
                     "DROP TABLE IF EXISTS DiskandRam CASCADE;"
                     "COMMIT;")
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Query(queryID, purpose,size) VALUES({id}, {purpose}, {size})").\
            format(id=sql.Literal(query.getQueryID()),purpose=sql.Literal(query.getPurpose()),
                   size=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    conn = None
    query = Query.badQuery()
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT * FROM Query WHERE queryID={id}"). \
            format(id=sql.Literal(queryID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
        if(rows_effected==0):
            return query
        query.setQueryID(res[0]['queryID'])
        query.setPurpose(res[0]['purpose'])
        query.setSize(res[0]['size'])
        return query
    except DatabaseException as e:
        return query
    except Exception as e:
        return query
    finally:
        conn.close()


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("BEGIN;"
                            "UPDATE Disk SET free_space= free_space+{size} WHERE diskID IN (SELECT diskID FROM DiskandQuery WHERE queryID={id});"
                            "DELETE FROM Query WHERE queryID={id};"
                            "COMMIT;"). \
            format(size=sql.Literal(query.getSize()),id=sql.Literal(query.getQueryID()))
        rows_effected, _ = conn.execute(sql_query)
    except DatabaseException as e:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(diskID,company,speed,free_space,cost) "
                        "VALUES({id}, {company}, {speed}, {free_space},{cost})"). \
            format(id=sql.Literal(disk.getDiskID()), company=sql.Literal(disk.getCompany()),
                   speed=sql.Literal(disk.getSpeed()), free_space=sql.Literal(disk.getFreeSpace()),
                   cost=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK



def getDiskProfile(diskID: int) -> Disk:
    conn = None
    disk = Disk.badDisk()
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT * FROM Disk WHERE diskID={id}"). \
            format(id=sql.Literal(diskID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
        if (rows_effected == 0):
            return disk
        disk.setDiskID(res[0]['diskID'])
        disk.setCompany(res[0]['company'])
        disk.setSpeed(res[0]['speed'])
        disk.setFreeSpace(res[0]['free_space'])
        disk.setCost(res[0]['cost'])

    except DatabaseException as e:
        return disk
    except Exception as e:
        return disk
    finally:
        conn.close()
    return disk


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("DELETE FROM Disk WHERE diskID={id};"). \
            format(id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(sql_query)
        conn.commit()
        if rows_effected==0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Ram(ramID, company,size) VALUES({id}, {company}, {size})"). \
            format(id=sql.Literal(ram.getRamID()), company=sql.Literal(ram.getCompany()),
                   size=sql.Literal(ram.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    ram = RAM.badRAM()
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT * FROM Ram WHERE ramID={id}"). \
            format(id=sql.Literal(ramID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
        if (rows_effected == 0):
            return ram
        ram.setRamID(res[0]['ramID'])
        ram.setCompany(res[0]['company'])
        ram.setSize(res[0]['size'])

    except DatabaseException as e:
        return ram
    except Exception as e:
        return ram
    finally:
        conn.close()
    return ram


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("DELETE FROM Ram WHERE ramID={id};"). \
            format(id=sql.Literal(ramID))
        rows_effected, _ = conn.execute(sql_query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("BEGIN;"
                        "INSERT INTO Disk(diskID,company,speed,free_space,cost) "
                        "VALUES({diskid}, {company}, {speed}, {free_space},{cost});"
                        "INSERT INTO Query(queryID, purpose,size) VALUES({queryid}, {purpose}, {size});"
                        "COMMIT;"). \
        format(diskid=sql.Literal(disk.getDiskID()), company=sql.Literal(disk.getCompany()),
                   speed=sql.Literal(disk.getSpeed()), free_space=sql.Literal(disk.getFreeSpace()),
                   cost=sql.Literal(disk.getCost()),queryid=sql.Literal(query.getQueryID()), purpose=sql.Literal(query.getPurpose()),
                   size=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(sql_query)
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("BEGIN;"
                            "INSERT INTO DiskandQuery(diskID,queryID,queryPurpose,querySize) "
                            "VALUES({diskid}, {queryid}, {querypurpose}, {querysize});"
                            "UPDATE Disk SET free_space=free_space-{querysize} WHERE diskID={diskid};"
                            "COMMIT;").\
                    format(diskid=sql.Literal(diskID),queryid=sql.Literal(query.getQueryID()),querypurpose=sql.Literal(query.getPurpose()),
                   querysize=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(sql_query)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("BEGIN;"
                            "UPDATE Disk SET free_space=free_space+{querysize} WHERE diskID={diskid} and EXISTS (SELECT *FROM DiskandQuery WHERE queryID={queryid} and diskID={diskid});"
                            "DELETE FROM DiskandQuery WHERE queryID={queryid} and diskID={diskid};"
                            "COMMIT;"). \
            format(diskid=sql.Literal(diskID), queryid=sql.Literal(query.getQueryID()),
                   querysize=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(sql_query)
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL(
                            "INSERT INTO DiskandRam(diskID,ramID) "
                            "VALUES({diskid}, {ramid})"). \
            format(diskid=sql.Literal(diskID), ramid=sql.Literal(ramID))
        rows_effected, _ = conn.execute(sql_query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL(
            "DELETE FROM DiskandRam WHERE diskID={diskid} AND ramID={ramid}"). \
            format(diskid=sql.Literal(diskID), ramid=sql.Literal(ramID))
        rows_effected, _ = conn.execute(sql_query)
        conn.commit()
        if rows_effected==0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK

def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn= None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT AVG(querySize) FROM DiskandQuery WHERE diskID={diskid}"). \
        format(diskid=sql.Literal(diskID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
        if res[0]['AVG'] == None:
            return 0
    except DatabaseException.ConnectionInvalid as e:
        return -1
    except Exception as e:
        return -1
    finally:
        conn.close()
    return res[0]['AVG']


def diskTotalRAM(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT SUM(size) FROM Ram WHERE ramID IN (SELECT ramID from DiskandRam WHERE diskID={diskid})"). \
            format(diskid=sql.Literal(diskID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
        if res[0]['SUM'] == None:
            return 0
    except DatabaseException.ConnectionInvalid as e:
        return -1
    except Exception as e:
        return -1
    finally:
        conn.close()
    return res[0]['SUM']


def getCostForPurpose(purpose: str) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL(
            "SELECT SUM(disk.cost * querysize) FROM disk INNER JOIN diskandquery ON(disk.diskid = diskandquery.diskid and diskandquery.querypurpose={purpose})"). \
            format(purpose=sql.Literal(purpose))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
        if res[0]['SUM'] == None:
            return 0
    except DatabaseException.ConnectionInvalid as e:
        return -1
    except Exception as e:
        return -1
    finally:
        conn.close()
    return res[0]['SUM']


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    list1=[]
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT queryID FROM Query WHERE size<=(SELECT free_space FROM Disk WHERE diskID={diskid}) ORDER BY queryID DESC LIMIT 5" ).\
            format(diskid=sql.Literal(diskID))
        rows_effected, res=conn.execute(sql_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    except Exception as e:
        return []
    finally:
        conn.close()
    for index in range(res.size()):
        list1.insert(index,res[index]['queryid'])
    return list1




def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    list1=[]
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT queryID FROM Query WHERE size<=(SELECT free_space FROM Disk WHERE diskID={diskid}) "
                            "and size<=(SELECT SUM(size) FROM Ram WHERE ramID IN (SELECT ramID from DiskandRam WHERE diskID={diskid}))"
                            "ORDER BY queryID ASC LIMIT 5"). \
            format(diskid=sql.Literal(diskID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    except Exception as e:
        return []
    finally:
        conn.close()
    for index in range(res.size()):
        list1.insert(index, res[index]['queryid'])
    return list1


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL(
            "SELECT company FROM Disk WHERE  diskID={diskid} and company=ALL(SELECT company from Ram WHERE ramID IN (SELECT ramID from DiskandRam WHERE diskID={diskid}))"). \
            format(diskid=sql.Literal(diskID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
        if rows_effected==0:
            return False
    except DatabaseException.ConnectionInvalid as e:
        return False
    except Exception as e:
        return False
    finally:
        conn.close()
    return True


def getConflictingDisks() -> List[int]:
    conn = None
    list1=[]
    try:
        conn = Connector.DBConnector()
        rows_effected, res = conn.execute("SELECT DISTINCT a.diskid FROM diskandquery AS a INNER JOIN diskandquery AS b ON a.queryid=b.queryid and a.diskid<>b.diskid ORDER BY a.diskid ASC")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    except Exception as e:
        return []
    finally:
        conn.close()
    for index in range(res.size()):
        list1.insert(index, res[index]['diskid'])
    return list1




def mostAvailableDisks() -> List[int]:
    conn = None
    list1=[]
    try:
        conn = Connector.DBConnector()
        rows_effected, res = conn.execute("SELECT C.did FROM (SELECT disk.diskid AS did ,query.queryid AS qid ,disk.speed AS ds  FROM Disk LEFT OUTER JOIN Query ON disk.free_space>=query.size) AS C "
                                          "GROUP BY C.did,C.ds "
                                          "ORDER BY COUNT(C.qid) DESC,C.ds DESC, C.did ASC LIMIT 5")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    except Exception as e:
        return []
    finally:
        conn.close()
    for index in range(res.size()):
        list1.insert(index,res[index]['did'])
    return list1


def getCloseQueries(queryID: int) -> List[int]:
    conn = None
    list1=[]
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT D.queryid"
                            " FROM (SELECT count(a.queryid),b.queryid "
                                    "FROM diskandquery AS a INNER JOIN diskandquery AS b ON a.diskid=b.diskid and a.queryid<>b.queryid "
                                    "WHERE a.queryid={id} GROUP BY b.queryid) AS D"
                            " WHERE D.count>=(SELECT COUNT(diskid)"
                                            "FROM diskandquery"
                                            " WHERE diskandquery.queryid={id})/(2*1.0) "
                            " UNION "
                            "SELECT queryID "
                            "FROM Query "
                            "WHERE queryID<>{id} and NOT EXISTS(SELECT queryID FROM DiskandQuery WHERE queryID={id}) "
                            "ORDER BY queryid ASC LIMIT 10"). \
            format(id=sql.Literal(queryID))
        rows_effected, res = conn.execute(sql_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    except Exception as e:
        return []
    finally:
        conn.close()
    for index in range(res.size()):
        list1.insert(index,res[index]['queryid'])
    return list1





    #addQueryToDisk(query3,1)
    #addQueryToDisk(query4,1)
    #print(getQueriesCanBeAddedToDiskAndRAM(1))
if __name__ == '__main__':
    dropTables()
    createTables()
    addDisk(Disk(1, "HP", 1, 1, 1))
    dropTables()
