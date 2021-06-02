import argparse

parser = argparse.ArgumentParser()
parser.add_argument('num_workers', type=int)
parser.add_argument('iterations', type=int)
args = parser.parse_args()

with open('horovod_experiment.sh', 'w') as f:
    f.write('#!/bin/sh\n\njavac -cp \".:jar_files/*:\" HOWorker.java\n')
    prefix = 'java -cp \".:jar_files/*:\" '
    for worker in range(args.num_workers):
        ID = str(worker)
        f.write(prefix + 'HOWorker ' + ID + ' ' + str(args.num_workers) + ' ' + str(args.iterations) + ' data/w' + ID + 'data.csv localhost:2181')
        if worker != args.num_workers - 1:
            f.write(' & ')
