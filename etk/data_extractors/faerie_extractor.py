import singleheap
import json
import sys


def ngrams(entity, n):
    for i in range(0, len(entity) - 1):
        yield entity[i:i+n]

class FaerieExtractor:

    def __init__(self, dictionary, token_size, threshold):
        with open(dictionary, 'rb') as f:
            self.dictionary = self.readDict((x for x in f), token_size)
        self.threshold = threshold
        self.token_size = token_size

    def readDict(self, dictfile, token_size):
        inverted_list = {}
        inverted_index = []
        entity_tokennum = {}
        inverted_list_len = {}
        entity_realid = {}
        entity_real = {}
        maxenl = 0
        n = token_size

        for i, line in enumerate(dictfile):
            entity_realid[i] = i
            entity_real[i] = line.strip()
            entity = entity_real[i].lower().strip()
            inverted_index.append(entity)  # record each entity and its id
            tokens = list(ngrams(entity, n))
            entity_tokennum[entity] = len(tokens)  # record each entity's token number
            if maxenl < len(tokens):
                maxenl = len(tokens)
            # build inverted lists for tokens
            tokens = list(set(tokens))
            for token in tokens:
                token_n = "".join(token)
                try:
                    inverted_list[token_n].append(i)
                    inverted_list_len[token_n] += 1
                except KeyError:
                    inverted_list[token_n] = []
                    inverted_list[token_n].append(i)
                    inverted_list_len[token_n] = 1
            i += 1
        return inverted_list, inverted_index, entity_tokennum, inverted_list_len, entity_realid, entity_real, maxenl


    def processDoc(self, line):
        inverted_list = self.dictionary[0]
        inverted_index = self.dictionary[1]
        entity_tokennum = self.dictionary[2]
        inverted_list_len = self.dictionary[3]
        entity_realid = self.dictionary[4]
        entity_real = self.dictionary[5]
        maxenl = self.dictionary[6]

        threshold = self.threshold
        n = self.token_size

        document_real = line

        jsonline = {}
        document = document_real.lower().strip()
        tokens = list(ngrams(document, n))
        heap = []
        keys = []
        los = len(tokens)
        # build the heap
        for i, token in enumerate(tokens):
            key = "".join(token)
            keys.append(key)
            try:
                heap.append([inverted_list[key][0], i])
            except KeyError:
                pass
        if heap:
            return_values_from_c = singleheap.getcandidates(heap, entity_tokennum, inverted_list_len, inverted_index,
                                                         inverted_list, keys, los, maxenl, threshold)
            jsonline["document"] = {}
            jsonline["document"]["value"] = document_real
            jsonline["entities"] = {}
            for value in return_values_from_c:
                temp = dict()
                temp["start"] = value[1]
                temp["end"] = value[2]+2
                temp["score"] = value[3]
                value_o = str(value[0])
                try:
                    entity_id = entity_realid[value_o]
                except KeyError:
                    value_o = value[0]
                    entity_id = entity_realid[value_o]
                try:
                    jsonline["entities"][entity_id]["candwins"].append(temp)
                except KeyError:
                    jsonline["entities"][entity_id] = {}
                    jsonline["entities"][entity_id]["value"] = entity_real[value_o]
                    jsonline["entities"][entity_id]["candwins"] = [temp]

        return jsonline


faerie = FaerieExtractor("sampledictionary.txt", 2, 0.8)
print faerie.processDoc("Los Angeles")
