from mrjob.step import MRStep
from mrjob.job import MRJob
class WordCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_word_count,
                    reducer=self.reducer_count)
        ]

    def mapper_get_word_count(self, _, line):
        words = line.split()
        for word in words:
            yield word, 1


    def reducer_count(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    WordCount.run()


