from memtable import MemTable
import uuid
import os
import bisect

class SimpleDb:
    KB = 1024
    MEMTABLE_THREASHOLD = 500 * KB
    DATA_DIR = "tmp/"
    KEY_SAMPLING_RATE = 100
    SEPARATOR = ","

    def __init__(self):
        self.memtable = MemTable()
        self.sstable_file_obj_list = []
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
        self._create_sstable_index(file_path)
        f = open(file_path, mode="r")
        self.sstable_file_obj_list.append(f)


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

        for sstable_file_obj in reversed(self.sstable_file_obj_list):
            result = self._search_sstable(sstable_file_obj, key)
            if result is not None:
                return result

        return None

    def _search_sstable(self, sstable_file_obj, key):
        sstable_index = self.sstable_index_dic[sstable_file_obj.name]
        key_list, offset_list = sstable_index
        key_pos = bisect.bisect_right(key_list, key)
        target_offset = offset_list[key_pos - 1]
        target_next_offset = None
        if key_pos < len(key_list):
            target_next_offset = offset_list[key_pos]

        sstable_file_obj.seek(target_offset)
        if target_next_offset is not None:
            while sstable_file_obj.tell() <= target_next_offset:
                line = sstable_file_obj.readline()
                sstable_key, value = line.rstrip().split(self.SEPARATOR)
                if sstable_key == key:
                    return value
        else:
            for line in sstable_file_obj:
                sstable_key, value = line.rstrip().split(self.SEPARATOR)
                if sstable_key == key:
                    return value

        return None
