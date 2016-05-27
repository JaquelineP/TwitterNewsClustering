class HashtagEvaluation():
    mymap = {}
    def in_other_cluster(self, id, hashtag):
        for key in self.mymap.keys():
            if (key == id):
                continue
            for hash in self.mymap[key]:
                if (hash == hashtag):
                    return True
        return False;

    def eval(self, cluster_ids, hashtags):
        wrong_within_cluster = 0
        wrong_cluster = 0
        empty_hashtag = 0
        for index, id in enumerate(cluster_ids):
            if (hashtags[index] == "" or hashtags[index] == None):
                empty_hashtag += 1
                continue
            if (self.in_other_cluster(id, hashtags[index])):
                wrong_cluster += 1
            if (not id in self.mymap):
                self.mymap[id] = [hashtags[index]]
                continue
            if (hashtags[index] in self.mymap[id]):
                if (self.mymap[id][0] == hashtags[index]):
                    continue
                wrong_within_cluster += 1
                continue

            self.mymap[id].append(hashtags[index])
            wrong_within_cluster += 1;

        clusters_withouth_hashtag = len(cluster_ids) - empty_hashtag;
        result = (wrong_within_cluster/clusters_withouth_hashtag,wrong_cluster/(clusters_withouth_hashtag**2), empty_hashtag)
        print("Within cluster %0.2f wrong cluster %0.2f empty hashtags %i" % result)
        return result

if __name__ == "__main__":
    evaluation = HashtagEvaluation()
    evaluation.eval([2, 1, 1], ["", "top", "brexit"])
