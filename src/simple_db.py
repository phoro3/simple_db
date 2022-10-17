from memtable import MemTable
import uuid
import os
import bisect

class SimpleDb:
    MB = 1024 * 1024
    MEMTABLE_THREASHOLD = 20 * MB
    DATA_DIR = "tmp/"
    KEY_SAMPLING_RATE = 100
    SEPARATOR = ","

    def __init__(self):
        self.memtable = MemTable()
        self.sstable_list = []
        self.sstable_index_dic = {} # {"key": (key_list, offset_list)}

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
        memtable_val = self.memtable.search(key)
        if memtable_val is not None:
            return memtable_val

        for sstable in reversed(self.sstable_list):
            result = self._search_sstable(sstable, key)
            if result is not None:
                return result

        return None

    def _search_sstable(self, sstable, key):
        sstable_index = self.sstable_index_dic[sstable]
        key_list, offset_list = sstable_index
        key_pos = bisect.bisect_right(key_list, key)
        target_offset = offset_list[key_pos - 1]
        target_next_offset = None
        if key_pos < len(key_list):
            target_next_offset = offset_list[key_pos]

        with open(sstable, "r") as f:
            f.seek(target_offset)
            if target_next_offset is not None:
                while f.tell() <= target_next_offset:
                    line = f.readline()
                    sstable_key, value = line.rstrip().split(self.SEPARATOR)
                    if sstable_key == key:
                        return value
            else:
                for line in f:
                    sstable_key, value = line.rstrip().split(self.SEPARATOR)
                    if sstable_key == key:
                        return value

        return None
