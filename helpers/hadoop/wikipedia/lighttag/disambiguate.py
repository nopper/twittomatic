import plyvel
from json import loads
from collections import defaultdict

from catalog.catalog import Catalog
from utils import profiled

class Disambiguator(object):
    def __init__(self):
        self.anchors_db = plyvel.DB('catalog/wikipedia-anchors')
        self.catalog = Catalog("catalog")
        self.cache = {}

    def get_senses(self, spot):
        # Here we convert a list of {'id': page_id, 'score': num_links}
        # to this format [(page_id, score), ..]

        senses = [
            sense for sense in loads(self.anchors_db.get(spot.encode('utf8')))
        ]

        # In this case we have a link_probability. Extract it
        if isinstance(senses[0], int):
            link_probability = senses.pop(0)
        else:
            link_probability = 1
        # Then we take the sum of all the scores and extract the probability
        total_score = float(sum(map(lambda x: x[1], senses)))
        senses = sorted(map(lambda x: (x[0], x[1] / total_score), senses), key=lambda x: x[1], reverse=True)
        #senses = map(lambda x: (x[0], 1 / float(len(senses))), senses)


        print "LINKPROB", spot, link_probability, total_score, len(senses)

        link_probability = max(link_probability, total_score, len(senses))
        link_probability = total_score / float(link_probability)

        return (link_probability, senses)

    def relatedness(self, id_a, id_b):
        a = self.catalog.get_entity('', id_a)
        b = self.catalog.get_entity('', id_b)

        score = a.related(b)

        self.cache[(id_a, id_b)] = score

        return score

    def vote_for(self, p_a, pg_b, prg_b, scores={}):
        """
        @param p_a the page_id of the sense to give to the spot a
        @param pg_b all the possible page_id of the sense to give to the spot b
        @param a list of floats containing the (p_b | b) probability
        """
        #with profiled("Collaborative voting for " + str(p_a) + " in %s"):
        relatedness = 0
        probabilities = 0

        for p_b, pr_b in zip(pg_b, prg_b):
            if p_b < p_a:
                relatedness += scores.get((p_b, p_a), 0)
            else:
                relatedness += scores.get((p_a, p_b), 0)


            #relatedness += self.relatedness(p_b, p_a)
            probabilities += pr_b


        score = (relatedness * probabilities) / len(pg_b)

        #print "Voting for", p_a, pg_b, score

        return score

    def precompute_related(self, senses):
        """
        @param a set of ids
        """
        with profiled("Precomputing scores in %s"):
            scores = {}

            operation = 0
            maxoperation = len(senses) / 2

            while operation < maxoperation:
                spot_senses = senses.pop(0)

                for other_senses in senses:
                    for source in spot_senses:
                        for dest in other_senses:
                            a, b = source, dest

                            if a > b:
                                a, b = b, a

                            if (a, b) in scores:
                                continue

                            scores[(a, b)] = self.relatedness(a, b)

                senses.append(spot_senses)
                operation += 1

            return scores


    def disambiguate(self, spots):
        db = self.anchors_db

        allsenses = []

        with profiled("Disambiguation in %s"):
            # First we get all the page connected to each spot

            pages = defaultdict(list)
            lp = {}

            index = 0
            while index < len(spots):
                spot = spots[index]
                link_prob, senses = self.get_senses(spot)

                allsenses.append(map(lambda x: x[0], senses))

                if link_prob > 0:
                    lp[spot], pages[spot] = link_prob, senses
                    #print "Link probability", spot, link_prob
                    index += 1
                else:
                    del spots[index]
                    print "Removing spot", spot, link_prob

            scores = self.precompute_related(allsenses)

            # Then we remove each spot and append it to the end
            # so we can apply the voting scheme

            candidates = defaultdict(list)

            for spot in spots:
                sense_ids = pages.pop(spot)

                #print "Voting for spot", spot

                for p_a, pr_a in sense_ids:
                    score = 0
                    counter = 0

                    for counter, (p_b, pg_pr_b) in enumerate(pages.items()):
                        pg_b = map(lambda x: x[0], pg_pr_b)
                        prg_b = map(lambda x: x[1], pg_pr_b)

                        score += self.vote_for(p_a, pg_b, prg_b, scores)

                    score /= float(counter + 1)
                    #print "Vote to spot", spot, p_a, "is", score

                    print "Link probability", lp[spot]

                    rho = (score + lp[spot]) / 2.0
                    candidates[spot].append((pr_a, rho, p_a))

                pages[spot] = sense_ids

            winning = {}

            for spot, champion_list in candidates.items():
                print "Spot", spot, "has the following:"


                for pr_a, score, p_a in sorted(champion_list, reverse=True, key=lambda x: x[1]):
                    #if score < 0.05:
                    #    continue

                    print "\tProability:", pr_a, "Score:", score, "Page id:", p_a, "Title", self.catalog.get_title(p_a)

                _, rho, wiki_id = sorted(champion_list, reverse=True, key=lambda x: x[1])[0]

                print _, rho, wiki_id

                #if rho > 0.05:
                winning[spot] = {
                    "rho": rho,
                    "id": wiki_id,
                    "title": self.catalog.get_title(wiki_id),
                    "portals": self.catalog.get_portals(wiki_id),
                }

            return winning


if __name__ == "__main__":
    dis = Disambiguator()
    spots = [
            "1938",
            "novembre",
            "fermi",
            "premio nobel",
            "studi",
            "settore",
            "fisica nucleare",
    ]
    spots = ["fermi", "studi", "premio nobel", "fisica nucleare"]
    spots = ["film", "tarantino"]

    #spots = ["uomini e donne", "programma televisivo", "merda"]

    dis.disambiguate(spots)
