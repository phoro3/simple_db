from avl_tree import AvlTree

class MemTable:
    def __init__(self) -> None:
        self.memtable = AvlTree()

    def set(self, key, value):
        self.memtable.set(key, value)

    def search(self, key):
        return self.memtable.search(key)
