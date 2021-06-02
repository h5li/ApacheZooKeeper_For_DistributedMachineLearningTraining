import argparse

parser = argparse.ArgumentParser()
parser.add_argument('num_workers', type=int)
parser.add_argument('iterations', type=int)
args = parser.parse_args()

with open('ps_experiment.sh', 'w') as f:
    f.write('#!/bin/sh\n\njavac -cp \".:jar_files/*:\" PSServer.java PSWorker.java\n')
    prefix = '\'java -cp \".:jar_files/*:\" '
    f.write('parallel --lb ::: ' + prefix + 'PSServer ' + str(args.num_workers) + ' ' + str(args.iterations) + ' localhost:2181\'')
    for worker in range(args.num_workers):
        ID = str(worker)
        f.write(' ' + prefix + 'PSWorker ' + ID + ' ' + str(args.iterations) + ' data/w' + ID + 'data.csv localhost:2181\'')
