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

        ########################################### addFileToDisk ###############################################

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
        self.assertEqual(Status.OK, result,  "Should be ok")

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




# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
