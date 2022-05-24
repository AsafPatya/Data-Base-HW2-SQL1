import unittest
import Solution
from Utility.Status import Status
from Tests.abstractTest import AbstractTest
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk

'''
    Simple test, create one of your own
    make sure the tests' names start with test_
'''


class Test(AbstractTest):
    ########################################### addFile, addDisk, addRAM #############################################
    def test_add_File(self) -> None:
        self.assertEqual(Status.OK, Solution.addFile(File(1, "wav", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "wav", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(3, "wav", 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFile(File(3, "wav", 10)),
                         "ID 3 already exists")

    def test_Disk(self) -> None:
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)),
                         "ID 1 already exists")

    def test_RAM(self) -> None:
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "Kingston", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "Kingston", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(3, "Kingston", 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAM(RAM(2, "Kingston", 10)),
                         "ID 2 already exists")

    ###################################### getFileByID, getDiskByID, getRAMByID ######################################

    def test_getFileByID(self):
        file1 = File(1, "wav", 10)
        Solution.addFile(file1)
        file2 = Solution.getFileByID(file1.getFileID())
        self.assertEqual(file1.getFileID(), file2.getFileID())
        self.assertEqual(file1.getSize(), file2.getSize())
        self.assertEqual(file1.getType(), file2.getType())

    def test_getDiskByID(self):
        disk1 = Disk(1, "DELL", 10, 10, 10)
        Solution.addDisk(disk1)
        disk2 = Solution.getDiskByID(disk1.getDiskID())
        self.assertEqual(disk1.getDiskID(), disk2.getDiskID())
        self.assertEqual(disk1.getCompany(), disk2.getCompany())
        self.assertEqual(disk1.getSpeed(), disk2.getSpeed())
        self.assertEqual(disk1.getFreeSpace(), disk2.getFreeSpace())
        self.assertEqual(disk1.getCost(), disk2.getCost())

    def test_getRAMByID(self):
        ram1 = RAM(1, "Kingston", 10)
        Solution.addRAM(ram1)
        ram2 = Solution.getRAMByID(ram1.getRamID())
        self.assertEqual(ram1.getRamID(), ram2.getRamID())
        self.assertEqual(ram1.getSize(), ram2.getSize())
        self.assertEqual(ram1.getCompany(), ram2.getCompany())

    ################################################# deleteFile #####################################################
    def test_deleteFile_regular_case(self):
        """

        :return:
        """
        file1 = File(1, "wav", 10)
        Solution.addFile(file1)
        file2 = Solution.getFileByID(file1.getFileID())
        self.assertEqual(file1.getFileID(), file2.getFileID(), "sanity check - id")
        self.assertEqual(file1.getSize(), file2.getSize(), "sanity check - size")
        self.assertEqual(file1.getType(), file2.getType(), "sanity check - type")
        Solution.deleteFile(file1)
        res = Solution.getFileByID(1)

        self.assertEqual(res.getFileID(), None, "id")
        self.assertEqual(res.getSize(), None, "size")
        self.assertEqual(res.getType(), None, "type")

    def test_deleteFile_file_not_exists(self):
        """

        :return:
        """
        file1 = File(1, "wav", 10)
        res = Solution.deleteFile(file1)
        self.assertEqual(Status.OK, res, "file not exists")

    def test_deleteFile_check_disk_size(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        first_size_disk = disk1.getFreeSpace()
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        Solution.addFileToDisk(file1, disk1.getDiskID())
        Solution.deleteFile(file1)
        new_size_disk = disk1.getFreeSpace()
        self.assertEqual(first_size_disk, new_size_disk, "checking size of disk")

    def test_deleteFile_check_disk_size_with_2_disks(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        disk2 = Disk(2, "DELL", 10, 10, 10)
        first_size_disk1 = disk1.getFreeSpace()
        first_size_disk2 = disk2.getFreeSpace()
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addFile(file1)
        Solution.addFileToDisk(file1, disk1.getDiskID())
        Solution.addFileToDisk(file1, disk2.getDiskID())
        Solution.deleteFile(file1)
        new_size_disk1 = disk1.getFreeSpace()
        new_size_disk2 = disk2.getFreeSpace()

        self.assertEqual(first_size_disk1, new_size_disk1, "checking size of disk")
        self.assertEqual(first_size_disk2, new_size_disk2, "checking size of disk")

    ################################################# deleteDisk #####################################################
    def test_deleteDisk_regular_case(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        Solution.addDisk(disk1)
        disk2 = Solution.getDiskByID(disk1.getDiskID())
        self.assertEqual(disk1.getDiskID(), disk2.getDiskID())
        self.assertEqual(disk1.getCompany(), disk2.getCompany())
        self.assertEqual(disk1.getSpeed(), disk2.getSpeed())
        self.assertEqual(disk1.getFreeSpace(), disk2.getFreeSpace())
        self.assertEqual(disk1.getCost(), disk2.getCost())

        Solution.deleteDisk(disk1.getDiskID())
        res = Solution.getDiskByID(1)

        self.assertEqual(res.getDiskID(), None, "id")
        self.assertEqual(res.getCompany(), None, "company")
        self.assertEqual(res.getSpeed(), None, "Speed")
        self.assertEqual(res.getFreeSpace(), None, "space")
        self.assertEqual(res.getCost(), None, "cost")

    def test_deleteDisk_check_file_stay(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        first_size_disk = disk1.getFreeSpace()
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        Solution.addFileToDisk(file1, disk1.getDiskID())
        Solution.deleteDisk(disk1.getDiskID())

        file2 = Solution.getFileByID(file1.getFileID())
        self.assertEqual(file1.getFileID(), file2.getFileID(), "sanity check - id")
        self.assertEqual(file1.getSize(), file2.getSize(), "sanity check - size")
        self.assertEqual(file1.getType(), file2.getType(), "sanity check - type")

    ################################################# addDiskAndFile ##################################################
    def test_addDiskAndFile(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        file1 = File(7, "wav", 8)
        Solution.addDiskAndFile(disk1, file1)

        disk2 = Solution.getDiskByID(disk1.getDiskID())
        self.assertEqual(disk1.getDiskID(), disk2.getDiskID())
        self.assertEqual(disk1.getCompany(), disk2.getCompany())
        self.assertEqual(disk1.getSpeed(), disk2.getSpeed())
        self.assertEqual(disk1.getFreeSpace(), disk2.getFreeSpace())
        self.assertEqual(disk1.getCost(), disk2.getCost())

        file2 = Solution.getFileByID(file1.getFileID())
        self.assertEqual(file1.getFileID(), file2.getFileID())
        self.assertEqual(file1.getSize(), file2.getSize())
        self.assertEqual(file1.getType(), file2.getType())

    ################################################# deleteRAM #####################################################
    def test_deleteRAM(self):
        """

        :return:
        """
        ram1 = RAM(1, "asaf", 1)
        Solution.addRAM(ram1)
        Solution.deleteRAM(ram1.getRamID())
        ram2 = Solution.getRAMByID(1)
        self.assertEqual(ram2.getRamID(), None)
        self.assertEqual(ram2.getSize(), None)
        self.assertEqual(ram2.getCompany(), None)

    def test_deleteRAM_check_disk_remain(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        Solution.addDisk(disk1)
        ram1 = RAM(3, "asaf", 1)
        Solution.addRAM(ram1)
        Solution.addRAMToDisk(3, 1)
        Solution.deleteRAM(ram1.getRamID())
        disk2 = Solution.getDiskByID(disk1.getDiskID())
        self.assertEqual(disk1.getDiskID(), disk2.getDiskID())
        self.assertEqual(disk1.getCompany(), disk2.getCompany())
        self.assertEqual(disk1.getSpeed(), disk2.getSpeed())
        self.assertEqual(disk1.getFreeSpace(), disk2.getFreeSpace())
        self.assertEqual(disk1.getCost(), disk2.getCost())

    ########################################### addFileToDisk #################################################

    def test_addFileToDisk_regular_case(self):
        """
        sainty check
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        result = Solution.addFileToDisk(file1, disk1.getDiskID())
        new_space_disk1 = Solution.getDiskByID(disk1.getDiskID()).getFreeSpace()
        expected_new_space = disk1.getFreeSpace() - file1.getSize()
        self.assertEqual(new_space_disk1, expected_new_space)
        self.assertEqual(Status.OK, result, "Should be ok")

    def test_addFileToDisk_file_not_exit_case(self):
        """
        when file2 not exist in DB
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        file1 = File(1, "wav", 8)
        file2 = File(2, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        result = Solution.addFileToDisk(file2, disk1.getDiskID())
        self.assertEqual(Status.NOT_EXISTS, result, "not exist")

    def test_addFileToDisk_not_exit_disk_case(self):
        """
        when disk2 not exit in DB
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        disk2 = Disk(2, "DELL", 10, 10, 10)
        file1 = File(1, "wav", 8)
        file2 = File(2, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        result = Solution.addFileToDisk(file1, disk2.getDiskID())
        self.assertEqual(Status.NOT_EXISTS, result, "not exist")

    def test_addFileToDisk_file_already_exist(self):
        """
        file1 already in disk1
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        Solution.addFileToDisk(file1, disk1.getDiskID())
        result = Solution.addFileToDisk(file1, disk1.getDiskID())
        self.assertEqual(Status.ALREADY_EXISTS, result, "already exits")

    def test_addFileToDisk_file_size_larger_than_free_space_on_disk(self):
        """
        size of file1 is larger than disk1
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        file1 = File(1, "wav", 15)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        result = Solution.addFileToDisk(file1, disk1.getDiskID())
        self.assertEqual(Status.BAD_PARAMS, result, "file space larger than disk space")

        ########################################### addRAMToDisk ###############################################

    def test_addRAMToDisk_regular_cae(self):
        """
        check the status value - check it OK
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        result = Solution.addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.OK, result, "should work")

    def test_addRAMToDisk_ram_not_exist(self):
        """
        check status when ram not exist
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addDisk(disk1)
        result = Solution.addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.NOT_EXISTS, result, "ram not exist")

    def test_addRAMToDisk_disk_not_exist(self):
        """
        check status when ram not exist
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addRAM(ram1)
        result = Solution.addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.NOT_EXISTS, result, "ram not exist")

    def test_addRAMToDisk_ram_already_exists(self):
        """
        check status when ram is already in disk
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
        result = Solution.addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.ALREADY_EXISTS, result, "ram already in disk")

    ########################################### removeFileFromDisk ###############################################

    def test_removeFileFromDisk_regular_check(self):
        """
        add file to disk and then remove it - check that the size are good
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        first_free_space_disk1 = disk1.getFreeSpace()
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        Solution.addFileToDisk(file1, disk1.getDiskID())
        new_space_disk = Solution.getDiskByID(disk1.getDiskID()).getFreeSpace()

        # sanity check
        self.assertEqual(new_space_disk, disk1.getFreeSpace() - file1.getSize(), "check adding")

        result = Solution.removeFileFromDisk(file1, disk1.getDiskID())
        new_space_disk_tag = Solution.getDiskByID(disk1.getDiskID()).getFreeSpace()
        self.assertEqual(new_space_disk_tag, first_free_space_disk1, "check removing")
        self.assertEqual(Status.OK, result, "should be OK")

    def test_removeFileFromDisk_file_not_exists(self):
        """
        should get OK when file not exists
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        first_free_space_disk1 = disk1.getFreeSpace()
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        result = Solution.removeFileFromDisk(file1, disk1.getDiskID())
        self.assertEqual(Status.OK, result, "should be OK")

        # sanity check
        new_space_disk_tag = Solution.getDiskByID(disk1.getDiskID()).getFreeSpace()
        self.assertEqual(first_free_space_disk1, new_space_disk_tag, "checking free space doesnt change")

    def test_removeFileFromDisk_disk_not_exists(self):
        """
        should get OK when file not exists
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        file1 = File(1, "wav", 8)
        Solution.addFile(file1)
        result = Solution.removeFileFromDisk(file1, disk1.getDiskID())
        self.assertEqual(Status.OK, result, "should be OK")

    def test_removeFileFromDisk_file_no_part_of_disk(self):
        """
        should get OK when file not exists in disk
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        first_free_space_disk1 = disk1.getFreeSpace()
        file1 = File(1, "wav", 8)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        result = Solution.removeFileFromDisk(file1, disk1.getDiskID())
        self.assertEqual(Status.OK, result, "should be OK")
        # sanity check
        new_space_disk_tag = Solution.getDiskByID(disk1.getDiskID()).getFreeSpace()
        self.assertEqual(first_free_space_disk1, new_space_disk_tag, "checking free space doesnt change")

    ########################################### removeRAMFromDisk ###############################################

    def test_removeRAMFromDisk_regular_case(self):
        """
        cant check if it really removed the ram from disk, can only check the Status
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)

        Solution.addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
        result = Solution.removeRAMFromDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.OK, result, "should be OK")

    def test_removeRAMFromDisk_ram_not_exists(self):
        """
        cant check if it really removed the ram from disk, can only check the Status
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addDisk(disk1)

        result = Solution.removeRAMFromDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.NOT_EXISTS, result, "ram not exists")

    def test_removeRAMFromDisk_disk_not_exists(self):
        """
        cant check if it really removed the ram from disk, can only check the Status
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addRAM(ram1)

        result = Solution.removeRAMFromDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.NOT_EXISTS, result, "disk not exists")

    def test_removeRAMFromDisk_ram_not_part_of_disk(self):
        """
        can't check if it really removed the ram from disk, can only check the Status
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10, 10)
        ram1 = RAM(1, "wav", 15)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)

        result = Solution.removeRAMFromDisk(ram1.getRamID(), disk1.getDiskID())
        self.assertEqual(Status.NOT_EXISTS, result, "ram not part of disk")

    ########################################### averageFileSizeOnDisk ###########################################

    def test_averageFileSizeOnDisk_regular_case(self):
        """
        regular check
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 20)
        file3 = File(3, "wav", 30)
        file4 = File(4, "wav", 40)
        file5 = File(5, "wav", 50)
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFileToDisk(file1, disk1.getDiskID())
        Solution.addFileToDisk(file2, disk1.getDiskID())
        Solution.addFileToDisk(file3, disk1.getDiskID())
        Solution.addFileToDisk(file4, disk1.getDiskID())
        Solution.addFileToDisk(file5, disk1.getDiskID())
        result = Solution.averageFileSizeOnDisk(disk1.getDiskID())
        expected = 10 + 20 + 30 + 40 + 50
        expected = expected / 5

        self.assertEqual(expected, result, "should be equal")

    def test_averageFileSizeOnDisk_heavy_case(self):
        """
        a lot of files
        :return:
        """
        n = 10
        disk1 = Disk(1, "DELL", 10, 10000000, 10)
        Solution.addDisk(disk1)

        files = []
        for file_id in range(1, n + 1):
            files.append(File(file_id, "A", file_id))

        for file_id in range(1, n + 1):
            Solution.addFile(files[file_id - 1])

        for file_id in range(1, n + 1):
            Solution.addFileToDisk(files[file_id - 1], disk1.getDiskID())

        result = Solution.averageFileSizeOnDisk(disk1.getDiskID())
        expected = sum([i for i in range(1, n + 1)])
        expected = expected / n
        print(result)
        print(expected)

    def test_averageFileSizeOnDisk_divide_by_zero(self):
        """
        in case there is no files in the disk
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10000000, 10)
        Solution.addDisk(disk1)
        result = Solution.averageFileSizeOnDisk(disk1.getDiskID())
        self.assertEqual(0, result)

    def test_test_averageFileSizeOnDisk_disk_does_not_exists(self):
        """
        in case disk does not exist, we need to return 0
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10000000, 10)
        result = Solution.averageFileSizeOnDisk(disk1.getDiskID())
        self.assertEqual(0, result)

    ########################################### diskTotalRAM ###########################################
    def test_diskTotalRAM_regular_case(self):
        """
        test the regular case
        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10000000, 10)
        ram1 = RAM(1, "wav", 15)
        ram2 = RAM(2, "wav", 15)
        ram3 = RAM(3, "wav", 15)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAM(ram2)
        Solution.addRAM(ram3)
        Solution.addRAMToDisk(1, 1)
        Solution.addRAMToDisk(2, 1)
        Solution.addRAMToDisk(3, 1)
        result = Solution.diskTotalRAM(1)
        self.assertEqual(45, result, "test_diskTotalRAM")

    def test_diskTotalRAM_heavy_case(self):
        """

        :return:
        """
        n = 10
        disk1 = Disk(1, "DELL", 10, 10000000, 10)
        Solution.addDisk(disk1)

        rams = []
        for ram_id in range(1, n + 1):
            rams.append(RAM(ram_id, "A", ram_id))

        for ram_id in range(1, n + 1):
            Solution.addRAM(rams[ram_id - 1])

        for ram_id in range(1, n + 1):
            Solution.addRAMToDisk(rams[ram_id - 1].getRamID(), disk1.getDiskID())

        result = Solution.diskTotalRAM(disk1.getDiskID())
        expected = sum([i for i in range(1, n + 1)])
        print(result)
        print(expected)

    def test_diskTotalRAM_disk_not_exists(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 10000000, 10)
        result = Solution.diskTotalRAM(disk1.getDiskID())
        self.assertEqual(0, result)

    ########################################### getCostForType ###########################################
    def test_getCostForType_regular_case(self):
        """

        :return:
        """
        disk1_CPR = 9
        disk1 = Disk(1, "DELL", 10, 10000000, disk1_CPR)
        disk2 = Disk(2, "DELL", 10, 10000000, disk1_CPR)

        file1 = File(1, "A", 10)
        file2 = File(2, "A", 15)
        file3 = File(3, "A", 15)

        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)

        Solution.addFileToDisk(file1, disk1.getDiskID())
        Solution.addFileToDisk(file2, disk1.getDiskID())
        Solution.addFileToDisk(file3, disk2.getDiskID())

        result = Solution.getCostForType("A")
        expected = disk1_CPR * (file1.getSize() + file2.getSize() + file3.getSize())
        self.assertEqual(expected, result)

    def test_getCostForType_type_note_exists(self):
        """

        :return:
        """
        Type = "C"
        disk1_CPR = 9
        disk1 = Disk(1, "DELL", 10, 10000000, disk1_CPR)
        disk2 = Disk(2, "DELL", 10, 10000000, disk1_CPR)

        file1 = File(1, "A", 10)
        file2 = File(2, "A", 15)
        file3 = File(3, "A", 15)
        file4 = File(4, "B", 15)

        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)

        Solution.addFileToDisk(file1, disk1.getDiskID())
        Solution.addFileToDisk(file2, disk1.getDiskID())
        Solution.addFileToDisk(file3, disk2.getDiskID())
        Solution.addFileToDisk(file4, disk2.getDiskID())

        result = Solution.getCostForType(Type)
        expected = 0
        self.assertEqual(expected, result)

    ############################################ getFilesCanBeAddedToDisk #########################################
    def test_getFilesCanBeAddedToDisk_regular_case(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 20)
        file3 = File(3, "wav", 30)
        file4 = File(4, "wav", 40)
        file5 = File(5, "wav", 50)
        file6 = File(6, "wav", 50)
        files = [file1, file2, file3, file4, file5, file6]
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)
        res = []
        res = Solution.getFilesCanBeAddedToDisk(disk1.getDiskID())
        expected = [file.getFileID() for file in files]
        expected.sort(reverse=True)
        expected = expected[:5]
        for i in range(5):
            self.assertEqual(res[i], expected[i])

    def test_getFilesCanBeAddedToDisk_no_disk_id_exists(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        disk2 = Disk(2, "DELL", 10, 200, 10)

        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 20)
        file3 = File(3, "wav", 30)
        file4 = File(4, "wav", 40)
        file5 = File(5, "wav", 50)
        file6 = File(6, "wav", 50)
        files = [file1, file2, file3, file4, file5, file6]
        Solution.addDisk(disk1)
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)
        res = []
        res = Solution.getFilesCanBeAddedToDisk(disk2.getDiskID())
        expected = []
        self.assertEqual(len(res), len(expected))

    ############################################ getFilesCanBeAddedToDiskAndRAM #######################################
    def test_getFilesCanBeAddedToDiskAndRAM(self):
        """
        :return:
        """

        disk1 = Disk(1, "DELL", 10, 200, 10)
        ram1 = RAM(1, "wav", 10)
        ram2 = RAM(2, "wav", 20)
        ram3 = RAM(3, "wav", 30)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAM(ram2)
        Solution.addRAM(ram3)
        Solution.addRAMToDisk(1, 1)
        Solution.addRAMToDisk(2, 1)
        Solution.addRAMToDisk(3, 1)

        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 20)
        file3 = File(3, "wav", 30)
        file4 = File(4, "wav", 40)
        file5 = File(5, "wav", 100)
        file6 = File(6, "wav", 100)
        files = [file1, file2, file3, file4, file5, file6]
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)
        res = []
        res = Solution.getFilesCanBeAddedToDiskAndRAM(disk1.getDiskID())
        expected = [1, 2, 3, 4]
        for i in range(4):
            self.assertEqual(res[i], expected[i])

    def test_getFilesCanBeAddedToDiskAndRAM_case_disk_not_exists(self):
        """
        :return:
        """

        disk1 = Disk(1, "DELL", 10, 200, 10)
        disk2 = Disk(2, "DELL", 10, 200, 10)

        ram1 = RAM(1, "wav", 10)
        ram2 = RAM(2, "wav", 20)
        ram3 = RAM(3, "wav", 30)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAM(ram2)
        Solution.addRAM(ram3)
        Solution.addRAMToDisk(1, 1)
        Solution.addRAMToDisk(2, 1)
        Solution.addRAMToDisk(3, 1)

        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 20)
        file3 = File(3, "wav", 30)
        file4 = File(4, "wav", 40)
        file5 = File(5, "wav", 100)
        file6 = File(6, "wav", 100)
        files = [file1, file2, file3, file4, file5, file6]
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)
        res = []
        res = Solution.getFilesCanBeAddedToDiskAndRAM(2)
        expected = []
        self.assertEqual(len(res), len(expected))

    ############################################ isCompanyExclusive #######################################
    def test_isCompanyExclusive_regular_case_True(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        ram1 = RAM(1, "DELL", 10)
        ram2 = RAM(2, "DELL", 20)
        ram3 = RAM(3, "DELL", 30)
        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAM(ram2)
        Solution.addRAM(ram3)
        Solution.addRAMToDisk(1, 1)
        Solution.addRAMToDisk(2, 1)
        Solution.addRAMToDisk(3, 1)
        res = Solution.isCompanyExclusive(disk1.getDiskID())
        self.assertEqual(True, res)

    def test_isCompanyExclusive_regular_case_False(self):
        """

        :return:
        """
        disk1 = Disk(1, "AMAZON", 10, 200, 10)
        ram1 = RAM(1, "DELL", 10)
        ram2 = RAM(2, "DELL", 20)
        ram3 = RAM(3, "DELL", 30)
        ram4 = RAM(4, "DL", 30)

        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAM(ram2)
        Solution.addRAM(ram3)
        Solution.addRAM(ram4)
        Solution.addRAMToDisk(1, 1)
        Solution.addRAMToDisk(2, 1)
        Solution.addRAMToDisk(3, 1)
        Solution.addRAMToDisk(4, 1)

        res = Solution.isCompanyExclusive(disk1.getDiskID())
        self.assertEqual(False, res)

    def test_isCompanyExclusive_regular_case_False1(self):
        """

        :return:
        """
        disk1 = Disk(1, "AMAZON", 10, 200, 10)
        ram1 = RAM(1, "AMAZON", 10)
        ram2 = RAM(2, "DELL", 20)
        ram3 = RAM(3, "DELL", 30)
        ram4 = RAM(4, "DL", 30)

        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAM(ram2)
        Solution.addRAM(ram3)
        Solution.addRAM(ram4)
        Solution.addRAMToDisk(1, 1)
        Solution.addRAMToDisk(2, 1)
        Solution.addRAMToDisk(3, 1)
        Solution.addRAMToDisk(4, 1)

        res = Solution.isCompanyExclusive(disk1.getDiskID())
        self.assertEqual(False, res)

    def test_isCompanyExclusive_disk_doesnt_exists(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        res = Solution.isCompanyExclusive(1)
        self.assertEqual(False, res)

    def test_isCompanyExclusive_disk_doesnt_exists1(self):
        """

        :return:
        """
        disk1 = Disk(1, "AMAZON", 10, 200, 10)
        disk2 = Disk(1, "AMAZON", 10, 200, 10)
        ram1 = RAM(1, "AMAZON", 10)
        ram2 = RAM(2, "DELL", 20)
        ram3 = RAM(3, "DELL", 30)
        ram4 = RAM(4, "DL", 30)

        Solution.addDisk(disk1)
        Solution.addRAM(ram1)
        Solution.addRAM(ram2)
        Solution.addRAM(ram3)
        Solution.addRAM(ram4)
        Solution.addRAMToDisk(1, 1)
        Solution.addRAMToDisk(2, 1)
        Solution.addRAMToDisk(3, 1)
        Solution.addRAMToDisk(4, 1)

        res = Solution.isCompanyExclusive(disk2.getDiskID())
        self.assertEqual(False, res)

    def test_isCompanyExclusive_regular_case_True_no_files(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        Solution.addDisk(disk1)
        res = Solution.isCompanyExclusive(disk1.getDiskID())
        self.assertEqual(True, res)

    ############################################ getConflictingDisks #######################################
    def test_getConflictingDisks_regular_test(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        disk2 = Disk(2, "DELL", 10, 200, 10)
        disk3 = Disk(3, "DELL", 10, 200, 10)
        disk4 = Disk(4, "DELL", 10, 200, 10)

        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        Solution.addDisk(disk4)

        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 10)
        file3 = File(3, "wav", 10)
        file4 = File(4, "wav", 10)
        file5 = File(5, "wav", 10)
        file6 = File(6, "wav", 10)

        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)

        Solution.addFileToDisk(file1, 1)
        Solution.addFileToDisk(file1, 3)
        Solution.addFileToDisk(file2, 1)
        Solution.addFileToDisk(file2, 2)
        Solution.addFileToDisk(file3, 2)
        Solution.addFileToDisk(file4, 3)
        Solution.addFileToDisk(file5, 4)

        res = Solution.getConflictingDisks()
        self.assertEqual(3, len(res))
        self.assertEqual(3, res[2])
        self.assertEqual(2, res[1])
        self.assertEqual(1, res[0])

    def test_getConflictingDisks_empty_list(self):
        """

        :return:
        """
        disk1 = Disk(1, "DELL", 10, 200, 10)
        disk2 = Disk(2, "DELL", 10, 200, 10)
        disk3 = Disk(3, "DELL", 10, 200, 10)
        disk4 = Disk(4, "DELL", 10, 200, 10)

        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        Solution.addDisk(disk4)

        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 10)
        file3 = File(3, "wav", 10)
        file4 = File(4, "wav", 10)
        file5 = File(5, "wav", 10)
        file6 = File(6, "wav", 10)

        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)

        Solution.addFileToDisk(file1, 1)
        Solution.addFileToDisk(file2, 2)
        Solution.addFileToDisk(file3, 3)
        Solution.addFileToDisk(file4, 4)
        Solution.addFileToDisk(file5, 5)

        res = Solution.getConflictingDisks()
        self.assertEqual(0, len(res))

    def test_getConflictingDisks_empty_list_no_disks(self):
        """

        :return:
        """
        res = Solution.getConflictingDisks()
        self.assertEqual(0, len(res))

    def test_getConflictingDisks_big_system(self):
        """

        :return:
        """
        n = 50
        disks = [Disk(i, "DELL", 10, 200, 10) for i in range(1, n+1)]
        files = [File(i, "wav", 10) for i in range(1, n+1)]
        for i in range(n):
            Solution.addFile(files[i])
            Solution.addDisk(disks[i])
        for i in range(1, n):
            Solution.addFileToDisk(files[i], i)
        files.append(File(n+1, "wav", 10))
        Solution.addFile(files[n])
        for i in range(1, n+1):
            Solution.addFileToDisk(files[n], i)
        res = Solution.getConflictingDisks()
        expected = [i for i in range(1, n+1)]
        self.assertEqual(len(expected), len(res))
        for i in range(n):
            self.assertEqual(res[i], expected[i])

    ############################################ mostAvailableDisks #######################################
    def test_mostAvailableDisks_regular_case(self):
        disk1 = Disk(1, "DELL", 10, 1, 10)
        disk2 = Disk(2, "DELL", 10, 2, 10)
        disk3 = Disk(3, "DELL", 10, 3, 10)
        disk4 = Disk(4, "DELL", 10, 4, 10)
        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        Solution.addDisk(disk4)
        file1 = File(1, "wav", 1)
        file2 = File(2, "wav", 2)
        file3 = File(3, "wav", 3)
        file4 = File(4, "wav", 4)
        file5 = File(5, "wav", 5)
        file6 = File(6, "wav", 6)
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)
        res = Solution.mostAvailableDisks()
        expected = [4, 3, 2, 1]
        self.assertEqual(len(expected), len(res))
        for i in range(4):
            self.assertEqual(res[i], expected[i])

    def test_mostAvailableDisks_regular_case1(self):
        disk1 = Disk(1, "DELL", 10, 70, 70)
        disk2 = Disk(2, "DELL", 10, 5, 5)
        disk3 = Disk(3, "DELL", 10, 5, 5)
        disk4 = Disk(4, "DELL", 10, 5, 5)
        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        Solution.addDisk(disk4)
        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 20)
        file3 = File(3, "wav", 30)
        file4 = File(4, "wav", 40)
        file5 = File(5, "wav", 50)
        file6 = File(6, "wav", 60)
        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)
        res = Solution.mostAvailableDisks()
        expected = [1]
        self.assertEqual(len(expected), len(res), "length")
        for i in range(1):
            self.assertEqual(expected[i], res[i])

    def test_mostAvailableDisks_no_disks(self):
        res = Solution.mostAvailableDisks()
        self.assertEqual(0, len(res))

    def test_mostAvailableDisks_no_files(self):
        disk1 = Disk(1, "DELL", 10, 1, 10)
        disk2 = Disk(2, "DELL", 10, 2, 10)
        disk3 = Disk(3, "DELL", 10, 3, 10)
        disk4 = Disk(4, "DELL", 10, 4, 10)
        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        Solution.addDisk(disk4)
        res = Solution.mostAvailableDisks()
        self.assertEqual(0, len(res))

    ############################################ getCloseFiles #######################################
    def test_getCloseFiles_limit_to_10(self):
        disk1 = Disk(1, "DELL", 10, 1000, 10)
        disk2 = Disk(2, "DELL", 10, 1000, 10)
        disk3 = Disk(3, "DELL", 10, 1000, 10)
        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        file1 = File(1, "wav", 10)
        file2 = File(2, "wav", 20)
        file3 = File(3, "wav", 30)
        file4 = File(4, "wav", 40)
        file5 = File(5, "wav", 50)
        file6 = File(6, "wav", 60)
        file7 = File(7, "wav", 10)
        file8 = File(8, "wav", 20)
        file9 = File(9, "wav", 30)
        file10 = File(10, "wav", 40)
        file11 = File(11, "wav", 50)
        file12 = File(12, "wav", 60)

        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)
        Solution.addFile(file5)
        Solution.addFile(file6)
        Solution.addFile(file7)
        Solution.addFile(file8)
        Solution.addFile(file9)
        Solution.addFile(file10)
        Solution.addFile(file11)
        Solution.addFile(file12)

        Solution.addFileToDisk(file1, 1)
        Solution.addFileToDisk(file2, 1)
        Solution.addFileToDisk(file3, 1)
        Solution.addFileToDisk(file4, 1)
        Solution.addFileToDisk(file5, 1)
        Solution.addFileToDisk(file6, 1)
        Solution.addFileToDisk(file7, 1)
        Solution.addFileToDisk(file8, 1)
        Solution.addFileToDisk(file9, 1)
        Solution.addFileToDisk(file10, 1)
        Solution.addFileToDisk(file11, 1)
        Solution.addFileToDisk(file12, 1)

        res = Solution.getCloseFiles(1)
        expected = [i for i in range(2, 12)]
        self.assertEqual(len(expected), len(res), "length")
        for i in range(len(expected)):
            self.assertEqual(expected[i], res[i])

    def test_getCloseFiles_regular_case(self):
        disk1 = Disk(1, "DELL", 10, 1000, 10)
        disk2 = Disk(2, "DELL", 10, 1000, 10)
        disk3 = Disk(3, "DELL", 10, 1000, 10)
        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        file1 = File(1, "wav", 1)
        file2 = File(2, "wav", 2)
        file3 = File(3, "wav", 3)
        file4 = File(4, "wav", 3)

        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)

        Solution.addFileToDisk(file1, 1)
        Solution.addFileToDisk(file1, 2)
        Solution.addFileToDisk(file2, 1)
        Solution.addFileToDisk(file2, 3)
        Solution.addFileToDisk(file3, 2)
        Solution.addFileToDisk(file3, 3)
        Solution.addFileToDisk(file4, 3)

        res = Solution.getCloseFiles(1)
        expected = [2, 3]
        self.assertEqual(len(expected), len(res), "length")
        for i in range(len(expected)):
            self.assertEqual(expected[i], res[i])

    def test_getCloseFiles_given_file_not_save_in_any_disk(self):
        disk1 = Disk(1, "DELL", 10, 1000, 10)
        disk2 = Disk(2, "DELL", 10, 1000, 10)
        disk3 = Disk(3, "DELL", 10, 1000, 10)
        Solution.addDisk(disk1)
        Solution.addDisk(disk2)
        Solution.addDisk(disk3)
        file1 = File(1, "wav", 1)
        file2 = File(2, "wav", 2)
        file3 = File(3, "wav", 3)
        file4 = File(4, "wav", 3)

        Solution.addFile(file1)
        Solution.addFile(file2)
        Solution.addFile(file3)
        Solution.addFile(file4)

        # Solution.addFileToDisk(file1, 1)
        # Solution.addFileToDisk(file1, 2)
        Solution.addFileToDisk(file2, 1)
        Solution.addFileToDisk(file2, 3)
        Solution.addFileToDisk(file3, 2)
        Solution.addFileToDisk(file3, 3)
        Solution.addFileToDisk(file4, 3)

        res = Solution.getCloseFiles(1)
        expected = [2, 3, 4]
        self.assertEqual(len(expected), len(res), "length")
        for i in range(len(expected)):
            self.assertEqual(expected[i], res[i])

# *** DO NOT RUN EACH TEST MANUALLY ***


if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
