from simple_db import SimpleDb
import os

if __name__ == "__main__":
    data_dir_name = "tmp"
    file_names = os.listdir(data_dir_name)
    for file in file_names:
        os.remove(os.path.join(data_dir_name, file))

    db = SimpleDb()
    import random
    N = 10000
    for _ in range(N):
        key = random.randrange(N)
        value = random.randrange(N)
        db.set(key, value)
