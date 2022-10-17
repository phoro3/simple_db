from simple_db import SimpleDb
import os
from string import ascii_lowercase
import random

def create_random_string(length):
    rand_char_list = random.choices(ascii_lowercase, k=length)
    return ''.join(rand_char_list)


if __name__ == "__main__":
    data_dir_name = "tmp"
    file_names = os.listdir(data_dir_name)
    for file in file_names:
        os.remove(os.path.join(data_dir_name, file))

    db = SimpleDb()
    max_key_length = 8
    N = 250000
    answer = {}
    for _ in range(N):
        key_length = random.randrange(2, max_key_length)
        key = create_random_string(key_length)
        value = str(random.randrange(N))

        answer[key] = value
        db.set(key, value)

    print("finish inserting")
    print("len: ", len(answer.keys()))
    insertion_error = 0
    for i, item in enumerate(answer.items()):
        if i % 10000 == 0:
            print("current: ", i)
        key, value = item
        db_ret = db.search(key)
        if db_ret != value:
            insertion_error += 1
    print("insertion error: ", insertion_error)