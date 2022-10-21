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
    SSTABLE_THREASHOLD = 3

    def __init__(self):
        self.memtable = MemTable()
        self.sstable_file_obj_list = []
        self.sstable_index_dic = {} # {"key": (key_list, offset_list)}

    def set(self, key, value):
        self.memtable.set(key, value)

        if self.memtable.get_size() > self.MEMTABLE_THREASHOLD:
            self._write_memtable()
            self._clear_memtable()

        if len(self.sstable_file_obj_list) > self.SSTABLE_THREASHOLD:
            new_file_obj = self._merge_sstable()
            self._update_sstable_info(new_file_obj)

    def _create_file_path(self):
        return os.path.join(self.DATA_DIR, str(uuid.uuid4()))

    def _write_memtable(self):
        file_path = self._create_file_path()
        self.memtable.write(file_path, self.SEPARATOR)
        self._create_sstable_index(file_path)
        f = open(file_path, mode="r")
        self.sstable_file_obj_list.append(f)


    def _clear_memtable(self):
        self.memtable.clear()

    def _create_sstable_index(self, file_path):
        index_keys = []
        index_offsets = []
        key_counter = 0
        with open(file_path, mode="r") as f:
            while True:
                pos = f.tell()
                line = f.readline()
                if not line:
                    break
                key, _ = line.rstrip().split(self.SEPARATOR)
                if key_counter % self.KEY_SAMPLING_RATE == 0:
                    index_keys.append(key)
                    index_offsets.append(pos)
                key_counter += 1
        self.sstable_index_dic[file_path] = (index_keys, index_offsets)

    def _merge_sstable(self):
        # sstableのうち末尾2つをマージする
        first_file_obj = self.sstable_file_obj_list[0]
        first_file_obj.seek(0)
        second_file_obj = self.sstable_file_obj_list[1]
        second_file_obj.seek(0)

        new_file_obj = open(self._create_file_path(), "w")
        line1 = first_file_obj.readline()
        line2 = second_file_obj.readline()
        while line1 or line2:
            if not line1:
                new_file_obj.write(line2)
                line2 = second_file_obj.readline()
                continue
            elif not line2:
                new_file_obj.write(line1)
                line1 = first_file_obj.readline()
                continue

            key1, _ = line1.split(self.SEPARATOR)
            key2, _ = line2.split(self.SEPARATOR)
            if key1 < key2:
                new_file_obj.write(line1)
                line1 = first_file_obj.readline()
            elif key1 > key2:
                new_file_obj.write(line2)
                line2 = second_file_obj.readline()
            else:
                # keyが同じ場合は新しい方のファイルの内容を書き込む
                new_file_obj.write(line2)
                line1 = first_file_obj.readline()
                line2 = second_file_obj.readline()
        new_file_obj.flush()
        return new_file_obj

    def _update_sstable_info(self, new_file_obj):
        # 末尾2つのsstableを削除し、新しいsstableに置き換える
        first_file_obj = self.sstable_file_obj_list[0]
        second_file_obj = self.sstable_file_obj_list[1]
        self._remove_old_sstable(first_file_obj)
        self._remove_old_sstable(second_file_obj)

        # 新しいsstableのインデックスを作成
        new_file_name = new_file_obj.name
        new_file_obj.close()
        self._create_sstable_index(new_file_name)
        # readモードで開き直し、sstableのlistに追加
        self.sstable_file_obj_list.insert(0, open(new_file_name, "r"))

    def _remove_old_sstable(self, old_file_obj):
        old_file_name = old_file_obj.name
        self.sstable_file_obj_list.remove(old_file_obj)
        old_file_obj.close()
        del self.sstable_index_dic[old_file_name]
        os.remove(old_file_name)



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
