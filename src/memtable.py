from avl_tree import AvlTree

class MemTable:
    def __init__(self) -> None:
        self.memtable = AvlTree()

    def set(self, key, value):
        self.memtable.set(key, value)

    def search(self, key):
        return self.memtable.search(key)

    def get_size(self):
        return self.memtable.get_size()

    def write(self, file_path, separator=","):
        with open(file_path, mode="w") as f:
            f.write(self.memtable.to_str(separator))

    def clear(self):
        self.memtable = AvlTree()

    def get_keys(self):
        return self.memtable.get_keys()