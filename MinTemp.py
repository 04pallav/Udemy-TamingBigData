from mrjob.job import MRJob

class MRMinTemp(MRJob):
	def mapper(self, key, line):
		weatherStationID, date, kind, value, w, x, y, z = line.split(',')
		if (kind == "TMIN"):
			yield weatherStationID, float(value)/10

	def reducer(self, weatherStationID, minTemp):
		yield weatherStationID, min(minTemp)

if __name__ == "__main__":
	MRMinTemp.run()
