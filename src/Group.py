

class Group:

	groupNo = 0
	def __init__(self, pattern, types = None, setOfSignatures = None):
		self.setOfPatterns = {pattern}
		self.setOfTypes = types
		if setOfSignatures is not None:
			self.setOfSignatures = setOfSignatures
		Group.groupNo += 1
		self.groupNo = Group.groupNo
		#print "[NEW] GROUP"
		#self.printGroup()

	def addPattern(self, pattern):
		self.setOfPatterns.add(pattern)

	def getType(self):
		return self.setOfTypes

	def setType(self, types):
		if self.setOfTypes == None:
			self.setOfTypes = types
		else:
			self.setOfTypes.add(types)

	def getSetOfPatterns(self):
		return self.setOfPatterns

	def getGroupNo(self):
		return self.groupNo

	def setGroupNo(self, groupNo):
		self.groupNo = groupNo

	def printGroup(self):
		if self.setOfTypes != None:
			print("Type:\t\t%s" %self.setOfTypes)
		else:
			print("Type:\t\tNone")
		print("groupNo:\t%d" %self.groupNo)

	def removePattern(self, pattern):
		#print len(self.setOfPatterns)
		self.setOfPatterns.remove(pattern)
		#print len(self.setOfPatterns)