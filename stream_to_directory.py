import glob
import os
import time

directory = "tweets"                    # where to put the tweets
filename = "twitter_sample.dat"         # dump location
throughput = 50                         # amount of tweets dumped per second

def start_dumping():

    # clear output directory
    for f in glob.glob(directory + "/tweet_*.txt"):
        os.remove(f)

    time_dif = 1 / throughput
    id = 0
    t = time.time()
    start_time = t
    with open(filename, "r") as dump:
        while True:
            line = dump.readline()

            # empty line means we reached the end of the file
            if line == "":
                break

            # save tweet to seperate file    
            with open(directory + "/tweet_%i.txt" % id, "w") as tweet:
                tweet.write(line)

            id += 1
            t += time_dif
            current_time = time.time()
            if t > current_time:
                time.sleep(t - current_time)

    print("Target throughput: %i" % throughput)
    print("Actual throughput: %.2f" % (id / (time.time() - start_time)))

if __name__ == "__main__":
    start_dumping()