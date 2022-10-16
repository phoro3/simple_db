from avl_tree import AvlTree
import sys

class MemTable:
    def __init__(self) -> None:
        self.memtable = AvlTree()

    def set(self, key, value):
        self.memtable.set(key, value)

    def search(self, key):
        return self.memtable.search(key)

    def get_size(self):
        return self.memtable.get_size()

    def write(self, file_path):
        with open(file_path, mode="w") as f:
            f.write(self.memtable.to_str())

    def clear(self):
        self.memtable = AvlTree()