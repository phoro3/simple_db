from memtable import MemTable
import uuid
import os

class SimpleDb:
    KB = 1024
    MEMTABLE_THREASHOLD = 500 * KB
    DATA_DIR = "tmp/"
    KEY_SAMPLING_RATE = 100
    SEPARATOR = ","

    def __init__(self):
        self.memtable = MemTable()
        self.sstable_list = []
        self.sstable_index_dic = {}

    def set(self, key, value):
        self.memtable.set(key, value)

        if self.memtable.get_size() > self.MEMTABLE_THREASHOLD:
            self._write_memtable()
            self._clear_memtable()

    def _write_memtable(self):
        file_name = str(uuid.uuid4())
        file_path = os.path.join(self.DATA_DIR, file_name)
        self.memtable.write(file_path, self.SEPARATOR)
        self.sstable_list.append(file_path)
        self._create_sstable_index(file_path)

    def _clear_memtable(self):
        self.memtable.clear()

    def _create_sstable_index(self, file_path):
        all_memtable_keys = self.memtable.get_keys()
        sampled_memetable_keys = [key for i, key in enumerate(all_memtable_keys) if i % self.KEY_SAMPLING_RATE == 0]

        index_offsets = []
        with open(file_path, mode="r") as f:
            while True:
                pos = f.tell()
                line = f.readline()
                if not line:
                    break
                key, _ = line.rstrip().split(self.SEPARATOR)
                if key in sampled_memetable_keys:
                    index_offsets.append(pos)
        self.sstable_index_dic[file_path] = (sampled_memetable_keys, index_offsets)

    def search(self, key):
        return self.memtable.search(key)
