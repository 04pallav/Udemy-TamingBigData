from mrjob.job import MRJob

class MRFriendsByAge(MRJob):
	def mapper(self, key, line):
		userID, name, age, numberOfFriends = line.split(',')
		yield age, float(numberOfFriends)

	def reducer(self, age, numberOfFriends):
		count, total = reduce(lambda (count, total), n : (count+1, total+n), numberOfFriends, (0,0))
		yield age, total/count

if __name__ == "__main__":
	MRFriendsByAge.run()
