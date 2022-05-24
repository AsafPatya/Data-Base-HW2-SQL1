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
        conn.execute("Begin; CREATE TABLE File( "
                     "file_id INTEGER PRIMARY KEY NOT NULL CHECK(file_id > 0), "
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
                     "disk_id INTEGER,"
                     "RAM_id INTEGER, "
                     "FOREIGN KEY (disk_id) REFERENCES Disk(disk_id) ON DELETE CASCADE, "
                     "FOREIGN KEY (RAM_id) REFERENCES RAM(RAM_id) ON DELETE CASCADE, "
                     "PRIMARY KEY (disk_id, RAM_id)); "

                     "CREATE TABLE DiskFile("
                     "disk_id INTEGER,"
                     "file_id INTEGER, "
                     "FOREIGN KEY (disk_id) REFERENCES Disk(disk_id) ON DELETE CASCADE, "
                     "FOREIGN KEY (file_id) REFERENCES File(file_id) ON DELETE CASCADE, "
                     "PRIMARY KEY (disk_id, file_id));"

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
        conn.execute("TRUNCATE File, Disk,RAM ,DiskRAM,DiskFile")
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
                     "DROP TABLE IF EXISTS File CASCADE;"
                     "DROP TABLE IF EXISTS Disk CASCADE;"
                     "DROP TABLE IF EXISTS RAM CASCADE;"
                     "DROP TABLE IF EXISTS DiskRAM CASCADE;"
                     "DROP TABLE IF EXISTS DiskFile CASCADE;"
                     "DROP TABLE IF EXISTS Temp CASCADE;"

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
        query = sql.SQL("INSERT INTO File(file_id,file_type,file_size) "
                        "VALUES({id},{type},{size})").format(id=sql.Literal(file.getFileID()),
                                                             type=sql.Literal(file.getType()),
                                                             size=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return Status.BAD_PARAMS
    finally:
        # will happen any way after code try termination or exception handling
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
    """
    Deletes a file from the database.
    Deleting a file will delete it from everywhere as if it never existed.
    Input: file to be deleted.

    Note: do not forget to adjust the free space on disk if the file is saved on one (later on).
    :param file:
    :return:
    Output: Status with the following conditions:
    * OK in case of success or if file does not exist (ID wise).
    * ERROR in case of a database error

                        "CREATE VIEW DISK_FREE_SPACE AS "
                        "SELECT disk_free_space "
                        "FROM Disk "
                        "WHERE disk_id = {disk_id}; "
    """
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"

                        "CREATE VIEW FILE_SIZE AS "
                        "SELECT file_size "
                        "FROM File "
                        "WHERE file_id = {file_id}; "

                        "CREATE VIEW DISK_HAVING_FILE AS "
                        "SELECT disk_id "
                        "FROM DiskFile "
                        "WHERE file_id = {file_id}; "

                        "UPDATE Disk "
                        "SET disk_free_space = disk_free_space + (SELECT SUM(file_size) FROM FILE_SIZE) "
                        "WHERE Disk.disk_id IN (SELECT disk_id FROM DISK_HAVING_FILE);"

                        "DELETE FROM File "
                        "WHERE  file_id = {file_id} ;"

                        "COMMIT;").format(file_id=sql.Literal(file.getFileID()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.OK
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.OK
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.OK
    except Exception as e:
        print(e)
        return Status.OK
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(disk_id, disk_company, disk_speed, disk_free_space, disk_CPB) "
                        "VALUES({id}, {company}, {speed}, {free_space}, {CPB}) "
                        ).format(
            id=sql.Literal(disk.getDiskID()),
            company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            free_space=sql.Literal(disk.getFreeSpace()),
            CPB=sql.Literal(disk.getCost()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return Status.BAD_PARAMS
    finally:
        # will happen any way after code try termination or exception handling
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
    """
    Deletes a disk from the database.
    Deleting a disk will delete it from everywhere as if he/she never existed.
    Input: disk ID to be deleted.

    :param diskID:
    :return:
    Output: Status with the following conditions:
    * OK in case of success
    * NOT_EXISTS if disk does not exist.
    * ERROR in case of a database error
    """
    conn = None
    rows_effected, result = 0, Connector.ResultSet()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk "
                        "WHERE  Disk.disk_id = {disk_id} ").format(disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR

    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.ERROR

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.ERROR

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.ERROR

    except Exception as e:
        print(e)
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()

    if rows_effected == 0:
        return Status.NOT_EXISTS

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
        return Status.BAD_PARAMS
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def addRAM(ram: RAM) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(RAM_id,RAM_size,RAM_company)"
                        " VALUES({id} ,{size},{company})").format(id=sql.Literal(ram.getRamID()),
                                                                  company=sql.Literal(ram.getCompany()),
                                                                  size=sql.Literal(ram.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except Exception as e:
        print(e)
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
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
            return RAM(result.rows[0][0], result.rows[0][2], result.rows[0][1])
        return RAM.badRAM()


def deleteRAM(ramID: int) -> Status:
    """
    Deletes a RAM from the database.
    Deleting a RAM will delete it from everywhere as if it never existed.
    Input: RAM ID to be deleted.

    :param ramID:
    :return:
    Output: Status with the following conditions:
    * OK in case of success
    * NOT_EXISTS if RAM does not exist.
    * ERROR in case of a database error
    """
    conn = None
    rows_effected, result = 0, Connector.ResultSet()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM "
                        "WHERE  RAM.RAM_id = {RAM_id} ").format(RAM_id=sql.Literal(ramID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR

    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.ERROR

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.ERROR

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.ERROR

    except Exception as e:
        print(e)
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()

    if rows_effected == 0:
        return Status.NOT_EXISTS

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
    conn = None
    rows_effected = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO DiskFile(disk_id, file_id) "
                        "VALUES({disk_id} ,{file_id});"

                        "DELETE FROM DiskFile "
                        "WHERE  DiskFile.disk_id = {disk_id} "
                        "AND  DiskFile.file_id = {file_id};"

                        "INSERT INTO DiskFile(file_id,disk_id) "
                        "(SELECT File.file_id , Disk.disk_id "
                        "FROM File, Disk "
                        "WHERE  Disk.disk_id = {disk_id}"
                        "AND  File.file_id = {file_id}"
                        "AND  File.file_size <= Disk.disk_free_space); "

                        "UPDATE Disk "
                        "SET disk_free_space= disk_free_space - "

                        "(SELECT SUM(File.File_size)"
                        "FROM File "
                        "WHERE File.file_id = {file_id})"

                        "WHERE Disk.disk_id = {disk_id}"
                        "AND EXISTS "
                        "(SELECT file_id FROM File "
                        "WHERE File.file_id = {file_id});"

                        "COMMIT;").format(disk_id=sql.Literal(diskID), file_id=sql.Literal(file.getFileID()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.NOT_EXISTS

    except DatabaseException.CHECK_VIOLATION as e:
        # this is the case where file's size is greater than the free space on disk so there will be a violation
        # in the disk -> it will get value that is negative
        print(e)
        return Status.BAD_PARAMS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # case on of them not exist
        print(e)
        return Status.NOT_EXISTS

    except DatabaseException.UNIQUE_VIOLATION as e:
        # case the file already save on the disk
        print(e)
        return Status.ALREADY_EXISTS

    except Exception as e:
        print(e)
        return Status.BAD_PARAMS
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()

    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    """
    The file with file.ID is now removed from the disk with diskID.

    param file: The file with file.ID to remove from disk with diskID.
    param diskID:

    :return:Status with the following conditions:
    * OK in case of success (also if file/disk does not exist or file is not saved on the disk).
    * ERROR in case of a database error
    Note: do not forget to adjust the free space on disk.
    """
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"

                        "UPDATE Disk "
                        "SET disk_free_space= disk_free_space + "
                        "(SELECT SUM(File.file_size)"
                        "FROM File "
                        "WHERE File.file_id = {file_id})"
                        "WHERE Disk.disk_id = {disk_id}"
                        "AND EXISTS "
                        "(SELECT * FROM DiskFile "
                        "WHERE DiskFile.file_id = {file_id} AND DiskFile.disk_id = {disk_id});"

                        "DELETE FROM DiskFile "
                        "WHERE  file_id = {file_id} AND disk_id = {disk_id} ;"

                        "COMMIT;").format(disk_id=sql.Literal(diskID), file_id=sql.Literal(file.getFileID()))
        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.OK

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.OK

    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.OK

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.OK

    except Exception as e:
        print(e)
        return Status.OK

    finally:
        # will happen any way after code try termination or exception handling
        conn.close()

    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    """
    The RAM with ramID is now a part of the disk with diskID.

    param ramID: The RAM with ramID which is now a part of the disk with diskID.
    :param diskID:
    :return:Status with the following conditions:
    * OK in case of success.
    * NOT_EXISTS if RAM/disk does not exist.
    * ALREADY_EXISTS if the RAM already a part of the disk.
    * ERROR in case of a database error
    """
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO DiskRAM(disk_id, RAM_id) "
                        "VALUES({disk_id} ,{ram_id});"

                        "DELETE FROM DiskRAM "
                        "WHERE  DiskRAM.disk_id = {disk_id} "
                        "AND  DiskRAM.ram_id = {ram_id};"

                        "INSERT INTO DiskRAM(disk_id,RAM_id) "
                        "(SELECT Disk.disk_id, RAM.RAM_id  "
                        "FROM Disk, RAM "
                        "WHERE  Disk.disk_id = {disk_id} AND RAM.RAM_id = {ram_id}); "
                        "COMMIT;").format(disk_id=sql.Literal(diskID), ram_id=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # NOT_EXISTS if RAM/disk does not exist.
        print(e)
        return Status.NOT_EXISTS

    except DatabaseException.UNIQUE_VIOLATION as e:
        # ALREADY_EXISTS if the RAM already a part of the disk.
        print(e)
        return Status.ALREADY_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        # ERROR in case of a database error
        print(e)
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)

    except Exception as e:
        print(e)

    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    """
    The RAM with ramID is now removed from the disk with diskID.
    Input: The RAM with ramID to remove from disk with diskID.
    :param ramID:
    :param diskID:
    :return:Status with the following conditions:
    * OK in case of success
    * NOT_EXISTS if RAM/disk does not exist or RAM is not a part of disk.
    * ERROR in case of a database error
    """
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM DiskRAM "
                        "WHERE  ram_id = {ram_id} AND disk_id = {disk_id} ;").format(disk_id=sql.Literal(diskID),
                                                                                     ram_id=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # not suppose to come here
        print(e)
        return Status.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        # not suppose to come here
        print(e)
        return Status.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        # not suppose to come here
        print(e)
        return Status.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # not suppose to come here
        print(e)
        return Status.NOT_EXISTS
    except Exception as e:
        # not suppose to come here
        print(e)
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()

    if rows_effected > 0:
        return Status.OK

    else:
        return Status.NOT_EXISTS


def averageFileSizeOnDisk(diskID: int) -> float:
    """
    Returns the average size of the files saved on the disk with diskID.
    Input: disk's ID.

    :param diskID:
    :return:
    * The average size in case of success.
    * 0 in case of division by 0 or if ID does not exist.
    * -1 in case of other errors.
    """
    conn = None
    X = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(file_size) "
                        "FROM File "
                        "WHERE File.file_id "
                        "IN (SELECT file_id "
                        "FROM DiskFile "
                        "WHERE disk_id= {disk_id}"
                        "GROUP BY disk_id, file_id "
                        "HAVING disk_id = {disk_id})").format(disk_id=sql.Literal(diskID))

        rows_effected, X = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.CHECK_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.UNIQUE_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if X.rows[0][0] is None:
        return 0

    return X.rows[0][0]


def diskTotalRAM(diskID: int) -> int:
    """
    Returns the total amount of RAM available on diskID.
    Input: diskID of the requested disk.

    :param diskID:
    :return:
    * The sum in case of success.
    * 0 if the disk does not exist.
    * -1 in case of other errors.
    """
    conn = None
    X = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(RAM_size) "
                        "FROM RAM "
                        "WHERE RAM.RAM_id IN "
                        "(SELECT RAM_id "
                        "FROM DiskRAM "
                        "WHERE disk_id= {disk_id}"
                        "GROUP BY disk_id, RAM_id "
                        "HAVING disk_id = {disk_id})").format(disk_id=sql.Literal(diskID))
        rows_effected, X = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.CHECK_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.UNIQUE_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if X.rows[0][0] is None:
        return 0

    return X.rows[0][0]


def getCostForType(type: str) -> int:
    """
    Returns the total amount of money paid for saving type files across all disks. (money paid = cost per unit * size).
    Input: the name of the requested type.

    :param type:
    :return:
    * The sum in case of success.
    * 0 if the type does not exist.
    * -1 in case of other errors.
    """
    conn = None
    X = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"

                        "CREATE VIEW File_id_size AS "
                        "SELECT file_id ,file_size "
                        "FROM File "
                        "GROUP BY file_type, file_id "
                        "HAVING file_type = {file_type};"

                        "CREATE VIEW Disk_id_CPB AS "
                        "SELECT  disk_id ,disk_CPB "
                        "FROM Disk; "

                        "CREATE VIEW DiskIDS2 AS "
                        "SELECT file_size , disk_CPB "
                        "FROM ((DiskFile INNER JOIN File_id_size  "
                        "ON DiskFile.file_id = File_id_size.file_id)"
                        "INNER JOIN Disk_id_CPB ON DiskFile.disk_id = Disk_id_CPB.disk_id); "

                        "SELECT SUM(file_size* disk_CPB) "
                        "FROM DiskIDS2 "
                        "COMMIT;").format(file_type=sql.Literal(type))

        rows_effected, X = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.CHECK_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.UNIQUE_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # not suppose to come here
        print(e)
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if X.rows[0][0] is None:
        return 0

    return X.rows[0][0]


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    """
    Returns a List (up to size 5) of files’ IDs that can be added to the disk with diskID as singles - not all together
    (even if they’re already on the disk).
    The list should be ordered by IDs in descending order.
    Input: The diskID in question.
    Output:
    * List with the files’ IDs.
    * Empty List in any other case.
    :param diskID:
    :return:
    """

    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT file_id "
                        "FROM File "
                        "WHERE File.file_size <= (SELECT SUM(disk_free_space) "
                        "FROM Disk "
                        "WHERE Disk.disk_id = {disk_id}) "
                        "ORDER BY file_id DESC LIMIT 5").format(disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        # not suppose to come here
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except DatabaseException.CHECK_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except DatabaseException.UNIQUE_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except Exception as e:
        # not suppose to come here
        print(e)
        return []

    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return_list = []
    for x in result.rows:
        return_list.append(int(x[0]))
    return return_list


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    """
    Returns a List (up to size 5) of files’ IDs that can be added to the disk with diskID as singles, not all together
    (even if they’re already on the disk)
    and can also fit in the sum of all the RAMs that belong to the disk with diskID.
    The list should be ordered by IDs in ascending order.
    Input: The diskID in question.
    Output:

    :param diskID:
    :return:
     * List with the files’ IDs.
    * Empty List in any other case.
    """
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "CREATE VIEW RAM_PART_OF_DISK AS SELECT  RAM_id "
                        "FROM DiskRAM "
                        "GROUP BY disk_id, RAM_id "
                        "HAVING disk_id = {disk_id};"

                        "CREATE VIEW RAM_SIZED AS "
                        "SELECT RAM_size "
                        "FROM RAM INNER JOIN RAM_PART_OF_DISK "
                        "ON RAM.RAM_id = RAM_PART_OF_DISK.RAM_id; "

                        "CREATE VIEW DISK_FREE_SPACE AS "
                        "SELECT disk_free_space "
                        "FROM Disk "
                        "WHERE disk_id = {disk_id}; "

                        "CREATE VIEW RESULT AS SELECT file_id FROM File "
                        "WHERE file_size <= (SELECT SUM(disk_free_space) FROM DISK_FREE_SPACE)"
                        " AND file_size <= (SELECT SUM(RAM_size) FROM RAM_SIZED); "

                        "SELECT file_id FROM RESULT "
                        "ORDER BY file_id ASC LIMIT 5;"

                        "SELECT * FROM RESULT "
                        "COMMIT;").format(disk_id=sql.Literal(diskID))

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        # not suppose to come here
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except DatabaseException.CHECK_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except DatabaseException.UNIQUE_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # not suppose to come here
        print(e)
        return []

    except Exception as e:
        # not suppose to come here
        print(e)
        return []

    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return_list = []
    for x in result.rows:
        return_list.append(int(x[0]))
    return return_list


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        
                        "CREATE TABLE Temp("
                        "disk_id INTEGER,"
                        "FOREIGN KEY (disk_id) REFERENCES Disk(disk_id));"
                        "INSERT INTO Temp(disk_id) VALUES({disk_id});"
                        "DROP TABLE IF EXISTS Temp ;"


                        "CREATE VIEW RAM_IN_DISK_COMPANY AS "
                        "SELECT DISTINCT RAM_company "
                        "FROM RAM "
                        "WHERE RAM.RAM_id IN "
                        "(SELECT  RAM_id "
                        "FROM DiskRAM "
                        "GROUP BY disk_id, RAM_id "
                        "HAVING disk_id = {disk_id}); "
                        
                        "CREATE VIEW DISK_COMPANY_NAME AS "
                        "SELECT disk_company "
                        "FROM Disk "
                        "WHERE Disk.disk_id = {disk_id};"
                        
                        "CREATE VIEW RESULT AS "
                        "SELECT disk_company "
                        "FROM DISK_COMPANY_NAME, RAM_IN_DISK_COMPANY "
                        "WHERE DISK_COMPANY_NAME.disk_company != RAM_IN_DISK_COMPANY.RAM_company;"
                        
                        "SELECT COUNT(disk_company) "
                        "FROM RESULT "

                        "COMMIT;").format(disk_id=sql.Literal(diskID))

        rows_effected, result = conn.execute(query)
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
        return False
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if result.rows[0][0] != 0:
        return False

    return True


def getConflictingDisks() -> List[int]:
    """
    Returns a list containing conflicting disks' IDs (no duplicates).
    Disks are conflicting if and only if they save at least one identical file.
    The list should be ordered by diskIDs in ascending order.
    Input: None

    :return:
    Output:
    *List with the disks' IDs.
    *Empty List in any other case.
    """
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        
                        "CREATE VIEW FILE_COUNT_OF_DISKS AS "
                        "SELECT file_id, COUNT(disk_id) "
                        "FROM DiskFile "
                        "GROUP BY DiskFile.file_id; "
                        
                        "CREATE VIEW FILE_COUNT_GREATE_THAN_ONE AS "
                        "SELECT file_id "
                        "FROM FILE_COUNT_OF_DISKS "
                        "WHERE FILE_COUNT_OF_DISKS.count>1; "
                        
                        "CREATE VIEW RESULT AS "
                        "SELECT DISTINCT disk_id "
                        "FROM DiskFile "
                        "WHERE DiskFile.file_id IN (SELECT file_id FROM FILE_COUNT_GREATE_THAN_ONE) "
                        "ORDER BY disk_id ASC;"
                        
                        "SELECT * "
                        "FROM RESULT "
                        
                        "COMMIT;")

        rows_effected, result = conn.execute(query)
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

    return_list = []

    if result.rows:
        # the list is not empty
        for res in result.rows:
            return_list.append(int(res[0]))
        return return_list

    return []


def mostAvailableDisks() -> List[int]:
    """
    Returns a list of up to 5 disks' IDs that can save the most files (as singles).
    A disk can save a file if and only if the file’s size is not larger than the free space on disk
    (even if it’s already saved on the disk).
    The list should be ordered by:
    • Main sort by number of files in descending order.
    • Secondary sort by disk's speed in descending order.
    • Final sort by diskID in ascending order.
    Input: None

    :return:
    Output:
    *List with the disks' IDs that satisfy the conditions above (if there are less than 5 disks, return a List with the <5 disks).
    *Empty List in any other case.
    """
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        
                        "CREATE VIEW  DISK_ID_DISK_SPEED_NUMBER_OF_FILES AS "
                        "SELECT disk_id, disk_speed, count(file_id) "
                        "FROM Disk, File "
                        "GROUP BY Disk.disk_id, Disk.disk_speed, File.file_id "
                        "HAVING Disk.disk_free_space >= File.file_size;"
                        
                        "CREATE VIEW RESULT AS "
                        "SELECT disk_id, disk_speed, COUNT(count) "
                        "FROM DISK_ID_DISK_SPEED_NUMBER_OF_FILES "
                        "GROUP BY disk_id, disk_speed "
                        "ORDER BY count DESC, disk_speed DESC, disk_id ASC LIMIT 5; "

                        "SELECT disk_id FROM RESULT "
                        "COMMIT;")

        rows_effected, result = conn.execute(query)
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
    ret_list=[]
    if (result.rows):
        for res in result.rows:
            ret_list.append(int(res[0]))
        return ret_list
    return []

def getCloseFiles(fileID: int) -> List[int]:
    return []


if __name__ == '__main__':
    dropTables()
    #
    # createTables()
    # disk1 = Disk(1, "DELL", 10, 200, 10)
    # ram1 = RAM(1, "DELL", 10)
    # ram2 = RAM(2, "DELL", 20)
    # ram3 = RAM(3, "DELL", 30)
    # # addDisk(disk1)
    # # addRAM(ram1)
    # # addRAM(ram2)
    # # addRAM(ram3)
    # # addRAMToDisk(1, 1)
    # # addRAMToDisk(2, 1)
    # # addRAMToDisk(3, 1)
    # res = isCompanyExclusive(disk1.getDiskID())

    # ram1 = RAM(1, "asaf", 1)
    # addRAM(ram1)
    # deleteRAM(ram1.getRamID())
    #
    # disk1 = Disk(1, "DELL", 10, 10, 10)
    # disk2 = Disk(2, "DELL", 10, 10, 10)
    # first_size_disk1 = disk1.getFreeSpace()
    # first_size_disk2 = disk2.getFreeSpace()
    # file1 = File(1, "wav", 8)
    # addDisk(disk1)
    # addDisk(disk2)
    # addFile(file1)
    # addFileToDisk(file1, disk1.getDiskID())
    # addFileToDisk(file1, disk2.getDiskID())
    # deleteFile(file1)

    # clearTables()
    #
    # n = 10
    # disk1 = Disk(1, "DELL", 10, 100000, 10)
    # addDisk(disk1)
    #
    # files = []
    # for file_id in range(1, n+1):
    #     files.append(File(file_id, "A", file_id))
    #
    # for file_id in range(1, n+1):
    #     addFile(files[file_id-1])
    #
    # for file_id in range(1, n+1):
    #     addFileToDisk(files[file_id-1], disk1.getDiskID())
    #
    # result = averageFileSizeOnDisk(disk1.getDiskID())
    # expected = sum([i for i in range(1, n+1)])
    # expected = expected / n
    # print(result)
    # print(expected)

    #
    # disk1 = Disk(1, "DELL", 10, 200, 10)
    # file1 = File(1, "wav", 10)
    # file2 = File(2, "wav", 20)
    # file3 = File(3, "wav", 30)
    # file4 = File(4, "wav", 40)
    # file5 = File(5, "wav", 50)
    #
    # addDisk(disk1)
    # addFile(file1)
    # addFile(file2)
    # addFile(file3)
    # addFile(file4)
    # addFile(file5)
    #
    # addFileToDisk(file1, disk1.getDiskID())
    # addFileToDisk(file2, disk1.getDiskID())
    # addFileToDisk(file3, disk1.getDiskID())
    # addFileToDisk(file4, disk1.getDiskID())
    # addFileToDisk(file5, disk1.getDiskID())
    #
    # result = averageFileSizeOnDisk(disk1.getDiskID())
    #
    # x = 10 + 20 + 30 + 40 + 50
    # x = x / 5
    # print(x)
    # print(result)

    # addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
    # result = removeRAMFromDisk(ram1.getRamID(), disk1.getDiskID())
    # #
    # # # #
    # disk1 = Disk(1, "DELL", 10, 10, 10)
    # first_free_space_disk1 = disk1.getFreeSpace()
    # file1 = File(1, "wav", 8)
    # addDisk(disk1)
    # addFile(file1)
    # addFileToDisk(file1, disk1.getDiskID())
    # new_space_disk = getDiskByID(disk1.getDiskID()).getFreeSpace()
    # result = removeFileFromDisk(file1, disk1.getDiskID())
    # new_space_disk_tag = getDiskByID(disk1.getDiskID()).getFreeSpace()

    # disk1 = Disk(1, "DELL", 10, 10, 10)
    # ram1 = RAM(1, "wav", 15)
    # addDisk(disk1)
    # addRAM(ram1)
    # addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
    # result = addRAMToDisk(ram1.getRamID(), disk1.getDiskID())

    # disk1 = Disk(1, "DELL", 10, 10, 10)
    # ram1 = RAM(1, "wav", 15)
    # addDisk(disk1)
    # result = addRAMToDisk(ram1.getRamID(), disk1.getDiskID())

    # disk1 = Disk(1, "DELL", 10, 10, 10)
    # ram1 = RAM(1, "wav", 15)
    # addDisk(disk1)
    # addRAM(ram1)
    # result = addRAMToDisk(ram1.getRamID(), disk1.getDiskID())

    # disk1 = Disk(4, "DELL", 10, 10, 10)
    # file1 = File(1, "wav", 8)
    # file2 = File(8, "wav", 8)
    # #
    # # addDisk(disk1)
    # # addFile(file1)
    #
    # addFileToDisk(file2, disk1.getDiskID())

    # result = addFileToDisk(file1, disk1.getDiskID())

    # clearTables()
    # file1 = File(1.5, "asaf", 1)
    # addFile(file1)
    # disk1 = Disk(1, "Elbit", 100, 100, 100)
    # disk2 = Disk(2, "Elbit", 100, 100, 100)
    #
    # ram1 = RAM(1, "eblbit", 100)
    # addDiskAndFile(disk1, file1)
    # addDiskAndFile(disk2, file1)

    # addFile(file1)
    # addDisk(disk1)
    # addRAM(ram1)
    # file12 = getFileByID(1)
    # disk12 = getDiskByID(2)
    # ram12 = getRAMByID(2)
