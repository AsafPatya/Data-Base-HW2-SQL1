from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql
from Utility.DBConnector import ResultSet
import psycopg2.extras


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("Begin; CREATE TABLE File("
                     "file_id INTEGER PRIMARY KEY NOT NULL CHECK(file_id > 0),"
                     "file_type TEXT NOT NULL, "
                     "file_size INTEGER NOT NULL CHECK(file_size >= 0));"

                     "CREATE TABLE Disk("
                     "disk_id INTEGER PRIMARY KEY NOT NULL CHECK(disk_id > 0),"
                     "disk_company TEXT NOT NULL,"
                     "disk_speed INTEGER NOT NULL CHECK(disk_speed> 0),"
                     "disk_free_space INTEGER NOT NULL CHECK(disk_free_space >= 0),"
                     "disk_CPB INTEGER NOT NULL CHECK(disk_CPB > 0));"

                     "CREATE TABLE RAM("
                     "RAM_id INTEGER PRIMARY KEY NOT NULL CHECK(RAM_id> 0),"
                     "RAM_size INTEGER NOT NULL CHECK(RAM_size> 0),"
                     "RAM_company TEXT NOT NULL );"

                     "CREATE TABLE DiskRAM("
                     "disk_id INTEGER NOT NULL,"
                     "RAM_id INTEGER NOT NULL);"

                     "CREATE TABLE DiskFile("
                     "disk_id INTEGER NOT NULL, "
                     "file_id INTEGER NOT NULL);"

                     "COMMIT;")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM File;"
                     "DELETE FROM Disk;"
                     "DELETE FROM RAM;"
                     "DELETE FROM DiskRAM;"
                     "DELETE FROM DiskFile;"
                     "COMMIT")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE File;"
                     "DROP TABLE Disk;"
                     "DROP TABLE RAM;"
                     "DROP TABLE DiskRAM;"
                     "DROP TABLE DiskFile;"
                     "COMMIT")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO File(file_id, file_type, file_size) "
                        "VALUES({id}, {type}, {size})").format(id=sql.Literal(file.getFileID()),
                                                               type=sql.Literal(file.getType()),
                                                               size=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS

    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def getFileByID(fileID: int) -> File:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM File WHERE File.file_id = {id}".format(id=fileID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT

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
        conn.close()
        if result.rows:
            return File(result.rows[0][0], result.rows[0][1], result.rows[0][2])
        return File.badFile()


def deleteFile(file: File) -> Status:
    return Status.OK


def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(disk_id, disk_company, disk_speed, disk_free_space, disk_CPB) "
                        "VALUES({id}, {company}, {speed}, {free_space}, {CPB})").format(
            id=sql.Literal(disk.getDiskID()),
            company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            free_space=sql.Literal(disk.getFreeSpace()),
            CPB=sql.Literal(disk.getCost()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS

    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS

    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS

    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Disk WHERE Disk.disk_id = {id}".format(id=diskID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT

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
        conn.close()
        if result.rows:
            return Disk(result.rows[0][0], result.rows[0][1], result.rows[0][2], result.rows[0][3], result.rows[0][4])
        return Disk.badDisk()


def deleteDisk(diskID: int) -> Status:
    return Status.OK


def addRAM(ram: RAM) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(RAM_id, RAM_size, RAM_company) "
                        "VALUES({id}, {size}, {company})").format(
            id=sql.Literal(ram.getRamID()),
            size=sql.Literal(ram.getSize()),
            company=sql.Literal(ram.getCompany()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS

    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS

    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS

    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM RAM WHERE RAM.RAM_id = {id}".format(id=ramID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT

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
        conn.close()
        if result.rows:
            return RAM(result.rows[0][0], result.rows[0][1], result.rows[0][2])
        return RAM.badRAM()


def deleteRAM(ramID: int) -> Status:
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO File(file_id, file_type, file_size) "
                        "VALUES({id_file1}, {type_file1}, {size_file1});"
                        "INSERT INTO Disk(disk_id, disk_company, disk_speed, disk_free_space, disk_CPB) "
                        "VALUES({id_disk1}, {company_disk1}, {speed_disk1}, {free_space_disk1}, {CPB_disk1});"
                        "COMMIT;").format(id_file1=sql.Literal(file.getFileID()),
                                          type_file1=sql.Literal(file.getType()),
                                          size_file1=sql.Literal(file.getSize()),
                                          id_disk1=sql.Literal(disk.getDiskID()),
                                          company_disk1=sql.Literal(disk.getCompany()),
                                          speed_disk1=sql.Literal(disk.getSpeed()),
                                          free_space_disk1=sql.Literal(disk.getFreeSpace()),
                                          CPB_disk1=sql.Literal(disk.getCost()))

        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS

    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
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
    # dropTables()
    # createTables()
    clearTables()
    file1 = File(1, "asaf", 1)
    disk1 = Disk(1, "Elbit", 100, 100, 100)
    disk2 = Disk(2, "Elbit", 100, 100, 100)

    ram1 = RAM(1, "eblbit", 100)
    addDiskAndFile(disk1, file1)
    addDiskAndFile(disk2, file1)

    # addFile(file1)
    # addDisk(disk1)
    # addRAM(ram1)
    # file12 = getFileByID(1)
    # disk12 = getDiskByID(2)
    # ram12 = getRAMByID(2)
