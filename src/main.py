from simple_db import SimpleDb
from base_db import BaseDb
import os
from string import ascii_lowercase
import random
import time

def create_random_string(length):
    rand_char_list = random.choices(ascii_lowercase, k=length)
    return ''.join(rand_char_list)

def prepare_data(N, min_key_length=2, max_key_length=8):
    answer = {}

    for _ in range(N):
        key_length = random.randrange(min_key_length, max_key_length)
        key = create_random_string(key_length)
        value = str(random.randrange(N))

        answer[key] = value
    return answer

def measure_db_performance(db, answer):
    insertion_start = time.time()

    for key, value in answer.items():
        db.set(key, value)
    print("finish inserting {}s".format(time.time() - insertion_start))

    search_start = time.time()
    insertion_error = 0
    for i, item in enumerate(answer.items()):
        key, value = item
        db_ret = db.search(key)
        if db_ret != value:
            insertion_error += 1
    print("finish search {}s".format(time.time() - search_start))
    print("insertion error: ", insertion_error)


if __name__ == "__main__":
    data_dir_name = "tmp"
    file_names = os.listdir(data_dir_name)
    for file in file_names:
        os.remove(os.path.join(data_dir_name, file))

    N = 25000
    answer = prepare_data(N)

    print("SimpleDb")
    db = SimpleDb()
    measure_db_performance(db, answer)

    print("BaseDb")
    db = BaseDb()
    measure_db_performance(db, answer)