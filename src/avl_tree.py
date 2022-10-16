import sys

class Node:
    def __init__(self, height, key, x):
        self.height = height
        self.key = key
        self.value = x
        self.lst = None
        self.rst = None

    def to_str(self):
        if self.height == 1:
            return str(self.key) + "," + str(self.value) + "\n"

        node_str = ""
        if self.lst is not None:
            node_str += self.lst.to_str()
        node_str += str(self.key) + "," + str(self.value) + "\n"
        if self.rst is not None:
            node_str += self.rst.to_str()
        return node_str

def height(t):
    return 0 if t is None else t.height

def bias(t):
    return height(t.lst) - height(t.rst)

# 左右の部分木の高さから、その木の高さを計算して修正する
def mod_height(t):
    t.height = 1 + max(height(t.lst), height(t.rst))

def rotate_left(t):
    u = t.rst
    t2 = u.lst

    u.lst = t
    t.rst = t2

    mod_height(u.lst)
    mod_height(u)
    return u

def rorate_right(t):
    v = t.lst
    t2 = v.rst

    v.rst = t
    t.lst = t2

    mod_height(v.rst)
    mod_height(v)
    return v

def rotate_left_right(t):
    t.lst = rotate_left(t.lst)
    return rorate_right(t)

def rotate_right_left(t):
    t.rst = rorate_right(t.rst)
    return rotate_left(t)


class AvlTree:
    def __init__(self):
        self.root = None
        self.active = False # 修正中かを示すフラグ
        self.size = 0

    def _balance_left(self, t):
        if not self.active: return t
        h = height(t)
        if bias(t) == 2:
            if bias(t.lst) >= 0:
                t = rorate_right(t)
            else:
                t = rotate_left_right(t)
        else:
            mod_height(t)
        self.active = (h != height(t))
        return t

    def _balance_right(self, t):
        if not self.active: return t
        h = height(t)
        if bias(t) == -2:
            if bias(t.rst) <= 0:
                t = rotate_left(t)
            else:
                t = rotate_right_left(t)
        else:
            mod_height(t)
        self.active = (h != height(t))
        return t

    def set(self, key, value):
        self.active = False
        self.root = self._insert(self.root, key, value)
        self.set_size(key, value)

    def _insert(self, t, key, value):
        if t is None:
            self.active = True
            return Node(1, key, value)
        elif key < t.key:
            t.lst = self._insert(t.lst, key, value)
            return self._balance_left(t)
        elif key > t.key:
            t.rst = self._insert(t.rst, key, value)
            return self._balance_right(t)
        else:
            # 値の更新
            t.value = value
            return t

    def is_member(self, key):
        t = self.root
        while t is not None:
            if key < t.key:
                t = t.lst
            elif key > t.key:
                t = t.rst
            else:
                return True
        return False

    def search(self, key):
        t = self.root
        while t is not None:
            if key < t.key:
                t = t.lst
            elif key > t.key:
                t = t.rst
            else:
                return t.value
        return None

    def to_str(self):
        return self.root.to_str()

    def set_size(self, key, value):
        total_size = sys.getsizeof(key) + sys.getsizeof(value)
        self.size += total_size

    def get_size(self):
        return self.size

# 動作確認用コード
if __name__ == "__main__":
    import random
    tree = AvlTree()
    N = 10
    insertion_erros = 0
    answer = {}
    for key in range(N):
        value = random.randrange(N)
        tree.set(key, value)
        answer[key] = value
    for key in answer:
        if (not tree.is_member(key)) or tree.search(key) != answer[key]:
            insertion_erros += 1
    print("insertion errors:", insertion_erros)
    print(tree.to_str())