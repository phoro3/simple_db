from memtable import MemTable

class SimpleDb:
    def __init__(self):
        self.memtable = MemTable()

    def set(self, key, value):
        self.memtable.set(key, value)

    def search(self, key):
        return self.memtable.search(key)

if __name__ == "__main__":
    db = SimpleDb()
    db.set(1, 2)
    print(db.search(1))