#!/usr/local/bin/python3
# encoding: utf-8
__author__ = "AlexChung"
__email__ = "achung@ischool.berkeley.edu"
__python_version = "3.2.2"

"""
Problem: Based on knowing the available space in the nodes of distributed file system and the size of 
files that need to be stored away.  Devise a distribution plan with the goal of making the 
total amount of data distributed to each storage node as balanced as reasonably possible. 

Program Description:
1) Parse the input files 
2) Initialize each storage node with zero count of byte being transferred over
3) Create a min-heap data structure to store the storage nodes and the value for
	ordering the heap tree has this format: (amount of data transferred, 1 / remaining available space)
4) The storage node with the least amount of data transferred will be first to received the next file
	if and only if the storage node has enough available space for the incoming file
5) Otherwise, keep popping the root node from the min-heap until one has enough capacity
6) Once a file has been planned to be sent to a storage node, update the node's properties of 
	data transferred and available space
7) Add the popped nodes back into the min-heap for the next file
8) The files iteration is in descending order from largest size to smallest
"""
"""
Assumptions:
* file is not fragmented
* no overhead accrued from file storage at each node
* no input node will be called NULL
* heap data structure has better (average) insertion and sorting time than quicksort in this case

Running this program, for example:
> python3 planFilesDistribution.py -f files.txt -n nodes.txt -o output.txt
"""

import re
import heapq
import sys

class StoreNode(object):
	"""
	StoreNode - storage node in the distributed file system
	"""

	def __init__(self, name, sizeAvail):
		self.name = name
		self.sizeAvail = int(sizeAvail) 
		self.filesSizeTransferred = 0  
		self.numFilesTransferred = 0  #for debugging
		
	def addFile(self, file):
		self.numFilesTransferred += 1  #for debugging
		self.filesSizeTransferred += file.size
		self.sizeAvail -= file.size
	
	#Generate the tuple key for sorting in the min-heap	
	def getTupleKey(self):
		"""
		Dividing 1 by the size available to promote nodes with higher capacity 
		to higher priority in a min-heap 
		"""
		return (self.filesSizeTransferred, 1/float(self.sizeAvail), self)
		
class File(object):
	"""
	File - data file to be distributed to the file system
	"""
	
	def __init__(self, name, size):
		self.name = name
		self.size = int(size)
		self.assignedNode = None
		
	def __lt__(self, other):
		return (self.size < other.size)
		
	def assignNode(self, node):
		self.assignedNode = node.name

#Regex pattern: Detect line starting with '#' or empty line 
skipPattern = re.compile("^(?:\s+)*#|(?:\s+)")

def importNodes(filename):
	try:
		#min-Heap data structure for the storage nodes
		nodeHeap = []
		
		#Read in the nodes files
		nodeFile = open(filename, 'r')
		for line in nodeFile:
			#Skip empty line or line starting with '#'
			if re.match(skipPattern, line):
				continue
			group = re.split('\s+', line)
			storeNode = StoreNode(group[0], group[1])
			heapq.heappush(nodeHeap, storeNode.getTupleKey())
		nodeFile.close()
		
		return nodeHeap
	except:
		print ("Error: Cannot read nodes-input from '", filename, "'")
		return -1
		
def importFiles(filename):
	try:
		#Read in the input files
		fileArray = list()
		inputFile = open(filename, "r")
		for line in inputFile:
			#Skip empty line or line starting with '#'
			if re.match(skipPattern, line):
				continue
			group = re.split('\s+', line)
			fileArray.append(File(group[0], group[1]))
		
		inputFile.close()
		
		#Sort the files by file size in descending order
		fileArray = sorted(fileArray, reverse=True)
				
		return fileArray
	except:
		print ("Error: Cannot read files-input from '", filename, "'")
		return -1
		
def planDistribution(nodeHeap, fileArray):
	for f in fileArray:
		try:
			#Store the popped storage nodes
			tempStoreNodeArray = list()

			#Remove and return the least active storage node.  Raise IndexError if empty
			while nodeHeap:
				n = heapq.heappop(nodeHeap)
				storeNode = n[2]
				tempStoreNodeArray.append(storeNode)

				#Check if storage node has enough space			
				if storeNode.sizeAvail >= f.size:
					#send file to storage node
					storeNode.addFile(f)			
					#update file's destination
					f.assignNode(storeNode)
					break

			#Add (updated) storage nodes back into heap 
			for tempN in tempStoreNodeArray:
				heapq.heappush(nodeHeap, tempN.getTupleKey())
		except:
			print ("Error: No storage node detected")

def reportResults(filename, nodeHeap, fileArray):
	
	if filename != None:
		try:
			#Format File Output
			outputFile = open(filename, 'w')
			for f in fileArray:
				if f.assignedNode is None:
					f.assignedNode = "NULL"	
				outputFile.write(f.name + " " + f.assignedNode + "\n")
			outputFile.close()
		except:
			print ("Error: Unable to create output file, '", filename, "'")
	else:
		#Standard Output
		for f in fileArray:
			if f.assignedNode is None:
				f.assignedNode = "NULL"	
			print (f.name + " " + f.assignedNode)
		
		"""
		print ("\n<!-----Storage Node Transferred File Size and Number----->\n")
		while nodeHeap:
			n = heapq.heappop(nodeHeap)
			storeNode = n[2]
			print (storeNode.name + " " + str(storeNode.sizeAvail) + " " + str(storeNode.filesSizeTransferred) +
			 " " + str(storeNode.numFilesTransferred))
		"""
		
def processPromptInput(argv):
	#Filenames
	inputFileName = None
	nodeFileName = None
	outputFileName = None
	
	inputError = False
	if ( (len(argv) == 2 and argv[1] == '-h')
		or
		(len(argv) < 2 or len(argv) % 2 == 0 or len(argv) > 7) ):
		inputError = True
	elif (len(argv) % 2 == 1):
		for i in range(1, len(argv), 2):
			inputKey = argv[i]
			inputValue = argv[i+1]
			
			if not re.match("^\s+$", inputValue):				
				if inputKey == '-f':
					inputFileName = inputValue
				elif inputKey == '-n':
					nodeFileName = inputValue
				elif inputKey == '-o':
					outputFileName = inputValue
				else:
					print ("Error: Unrecognized option key '", inputKey, "'")
					inputError = True
					break
			else:
				print ("Error: No filename is provided")
				inputError = True
				break

		#Check for required inputFiles
		if inputFileName == None or nodeFileName == None:
			print ("Error: Both input files for 'files' and 'nodes' are required")
			inputError = True
			
	#Usage Instruction Prompt
	if inputError:
		sys.stderr.write("Usage: python " + sys.argv[0] + "\n"
		 "\t-f filename: Input file for files, e.g. -f files.txt. Required.\n" +
		 "\t-n filename: Input file for nodes, e.g. -n nodes.txt. Required.\n" +
		 "\t[-o filename: Output file, e.g. -o result.txt. Optional] \n" +
		 "\n\t-h: Print usage information\n"
		)
		return -1
	
	return (inputFileName, nodeFileName, outputFileName)

def main(argv):
	#Check for calling error and Extract filenames
	fileReturnTuple = processPromptInput(argv)
	if fileReturnTuple != -1:
		#Filenames
		inputFileName = fileReturnTuple[0]
		nodeFileName = fileReturnTuple[1]
		outputFileName = fileReturnTuple[2]
		
		#Parse/Extract input files
		fileArray = importFiles(inputFileName)	
		nodeHeap = importNodes(nodeFileName)
		#Plan Distribution and Report
		if nodeHeap != -1 and fileArray != -1:	
			planDistribution(nodeHeap, fileArray)
			reportResults(outputFileName, nodeHeap, fileArray)
		
if __name__ == '__main__':
	sys.exit(main(sys.argv))


