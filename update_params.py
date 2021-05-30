import numpy as np
import os

w = np.zeros((785, 1))
if os.path.exists('params.npy'):
    with open('params.npy', 'rb') as f:
        w = np.load(f)

with open('grads.txt', 'r') as f:
    for i, line in enumerate(f):
        w[i][0] += float(line.strip())

with open('params.npy', 'wb') as f:
    np.save(f, w)