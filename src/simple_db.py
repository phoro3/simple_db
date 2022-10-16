from memtable import MemTable
import uuid
import os

class SimpleDb:
    KB = 1024
    MEMTABLE_THREASHOLD = 500 * KB
    DATA_DIR = "tmp/"

    def __init__(self):
        self.memtable = MemTable()

    def set(self, key, value):
        self.memtable.set(key, value)

        if self.memtable.get_size() > self.MEMTABLE_THREASHOLD:
            self._write_memtable()
            self._clear_memtable()

    def _write_memtable(self):
        file_name = str(uuid.uuid4())
        file_path = os.path.join(self.DATA_DIR, file_name)
        self.memtable.write(file_path)

    def _clear_memtable(self):
        self.memtable.clear()

    def search(self, key):
        return self.memtable.search(key)
