from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTable() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE IF NOT EXISTS File(fileID INTEGER UNIQUE NOT NULL, type TEXT NOT NULL, "
                     "size INTEGER NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS Disk(diskID INTEGER UNIQUE NOT NULL, company TEXT NOT NULL,"
                     " speed INTEGER NOT NULL, free_space TEXT NOT NULL, cost INTEGER NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS Ram(ramID INTEGER UNIQUE NOT NULL, company TEXT NOT NULL,"
                     " size INTEGER NOT NULL)")
        # conn.execute("INSERT INTO file(fileID, type, size) VALUES(1, 'asaf', 2)")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM file")
        conn.execute("DELETE FROM disk")
        conn.execute("DELETE FROM ram")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE file")
        conn.execute("DROP TABLE disk")
        conn.execute("DROP TABLE ram")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def addFile(file: File) -> Status:
    return Status.OK


def getFileByID(fileID: int) -> File:
    return File()


def deleteFile(file: File) -> Status:
    return Status.OK


def addDisk(disk: Disk) -> Status:
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> Status:
    return Status.OK


def addRAM(ram: RAM) -> Status:
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> Status:
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForType(type: str) -> int:
    return 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []


if __name__ == '__main__':
    createTable()
    clearTables()
    dropTables()