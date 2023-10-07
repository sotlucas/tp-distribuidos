import time

time_start = time.time()
with open("../temp/archivo.csv", "r") as f:
    for line in f:
        print(line.rstrip().encode())

print(f"Tiempo que tardó: {time.time() - time_start}")
