from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

# The character we wish to find the degree of separation
startID = 5306 # SpiderMan
targetID = 14 # ADAM 3,031

# Acumulator, used to signal when we find the target
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    # connections = [int(x) for x in fields[1:]]

    color = "WHITE"
    distance = 9999

    if (heroID == startID):
        color = "GRAY"
        distance = 0
    
    return (heroID, (connections, distance, color))

def createStartingRDD():
    inputFile = sc.textFile("./data/marvel/marvel_graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if color == "GRAY":
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = "GRAY"
            if targetID == connection:
                hitCounter.add(1)
            
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)
        
        # node processed, so color it black
        color = "BLACK"

    results.append((characterID, (connections, distance, color)))
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # Check if one is the original node with its connections
    # If so preserve them
    if len(edges1) > 0:
        edges.extend(edges1)
    elif len(edges2) > 0:
        edges.extend(edges2)
    
    # Preserve minimum distance
    if distance1 < distance:
        distance = distance1
    
    if distance2 < distance:
        distance = distance2
    
    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1
    
    return (edges, distance, color)

iterationRDD = createStartingRDD()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration + 1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done
    mapped = iterationRDD.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated
    # and that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if hitCounter.value > 0:
        print("Hit the target character! From " + str(hitCounter.value)
             + " different direction(s)")
        break
    
    # Reducer combines data for each character ID, preserving the darkest
    # color and shortes path.
    iterationRDD = mapped.reduceByKey(bfsReduce)
