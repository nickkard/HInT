

class Pattern:

	patternNo = 0
	def __init__(self, setOfProperties, instance, types = None):
		self.setOfProperties = setOfProperties
		self.setOfInstances = {instance}
		self.setOfTypes = types
		Pattern.patternNo += 1
		self.patternNo = Pattern.patternNo
		#print "[NEW] PATTERN"
		#self.printPattern()

	def getType(self):
		return self.setOfTypes

	def setType(self, types):
		if self.setOfTypes == None:
			self.setOfTypes = types
		else:
			self.setOfTypes.add(types)

	def addInstance(self, e):
		self.setOfInstances.add(e)

	def getSetOfProperties(self):
		return self.setOfProperties

	def getPatternNo(self):
		return self.patternNo

	def printPattern(self):
		if self.setOfTypes != None:
			print("Type:\t\t%s" %self.setOfTypes)
		else:
			print("Type:\t\tNone")
		print("patternNo:\t%d" %self.patternNo)

	def getSetOfInstances(self):
		return self.setOfInstances

