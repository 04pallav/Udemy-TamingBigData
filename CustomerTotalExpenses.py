from mrjob.job import MRJob
from mrjob.step import MRStep

import mrjob.protocol

from decimal import Decimal

class MRCustomerTotalExpenses(MRJob):
	INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol
	OUTPUT_PROTOCOL = mrjob.protocol.ReprProtocol

	def steps(self):
		return [
			MRStep(mapper=self.mapper, reducer=self.reducer),
			MRStep(mapper=self.mapper_sorter, reducer=self.reducer_swap),
		]

	def mapper(self, key, line):
		customerID, itemID, amount = line.split(',')
		yield customerID, Decimal(amount)

	def reducer(self, customerID, amounts):
		yield customerID, sum(amounts)

	def mapper_sorter(self, customerID, total):
		yield "{:9.2f}".format(total), customerID

	def reducer_swap(self, total, customers):
		for customer in customers:
			yield customer, total

if __name__ == "__main__":
	MRCustomerTotalExpenses.run()