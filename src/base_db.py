import os


class BaseDb:
    DATA_DIR = "tmp/"
    SEPARATOR = ","

    def __init__(self, file_name="database"):
        file_path = os.path.join(self.DATA_DIR, file_name)
        f = open(file_path, mode="w+")
        self.db_obj = f

    def set(self, key, value):
        self.db_obj.seek(0, 2)  #末尾に移動
        write_str = key + self.SEPARATOR + value
        self.db_obj.write(write_str + "\n")

    def search(self, key):
        self.db_obj.seek(0)
        ret_value = None
        for line in self.db_obj:
            k, v = line.rstrip().split(self.SEPARATOR)
            if k == key:
                ret_value = v
        return ret_value