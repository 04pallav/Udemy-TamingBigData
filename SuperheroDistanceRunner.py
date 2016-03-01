# run.py
from SuperheroDistance import MRSuperheroDistance

job = MRSuperheroDistance(args=["-v", "--strict-protocols", "../datasets/Marvel-Graph.txt"])

with job.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        print line