# run.py
from SuperheroDistance import MRPreProcess, MRBFSStep, MRResult
from collections import defaultdict

dst = "d/step-0"
job = MRPreProcess(args=["-v", "--strict-protocols", 
                         "--output-dir=" + dst, "-"])
with job.make_runner() as runner:
    runner.run()

done = False
step = 0
while not done:
    src = dst
    step += 1
    dst = "d/step-{:d}".format(step)
    job = MRBFSStep(args=["-v", "--strict-protocols", 
                             "--output-dir=" + dst, src])
    with job.make_runner() as runner:
        runner.run()

        counters = defaultdict(dict)
        for counter in runner.counters():
            counters.update(counter)
        print counters
        if 'found' in counters['superhero'] or 'touched' not in counters['superhero']:
            done = True


src = dst
job = MRResult(args=[src])
with job.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        print line.strip()
