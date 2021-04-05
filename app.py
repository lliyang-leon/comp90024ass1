import json
import re
import os
import sys
from mpi4py import MPI


def putKeywords(file):
    keywords = {}
    for i in range(len(file.splitlines())):
        keywords[re.split(r'\t+', file.splitlines()[i])[0].lower()] = int(re.split(r'\t+', file.splitlines()[i])[1])
    return keywords

def getArea(coordinate, areas):
    vChars = ["A","B","C","D"]
    outOfRangeY = -37.95
    outOfRangeX = 145.0

    horizon = [areas["C1"][0], areas["C1"][1], areas["C2"][1], areas["C3"][1], areas["C4"][1], areas["C5"][1]]
    vertical = [areas["A3"][3], areas["A3"][2], areas["B3"][2], areas["C3"][2], areas["D3"][2]]
    area = ""
    found = False

    if (coordinate == []):
        return ""
    # find the horizontal block of the coordinate
    for i in range(len(horizon)):
        if coordinate[0] == horizon[i]:
            if coordinate[0] == outOfRangeX and coordinate[1] < outOfRangeY:
                area = i + 1
            else:
                area = i
        if area == 0:
            area += 1
    if area == "":
        horizon.append(coordinate[0])
        horizon = sorted(horizon)
        area = horizon.index(coordinate[0])
        # remove out of range blocks
        if (area == 0 or area == 6):
            return ""
        horizon.remove(coordinate[0])
    # find the vertical block
    for i in range(len(vertical)):
        if coordinate[1] == vertical[i]:
            if coordinate[1] == outOfRangeY and coordinate[0] <= outOfRangeX:
                area = vChars[i - 1] + str(area)
            else:
                area = vChars[i] + str(area)
            found = True
    if found == False:
        vertical.append(coordinate[1])
        vertical = sorted(vertical, reverse=True)
        # remove out of range blocks
        if (vertical.index(coordinate[1]) == 0 or vertical.index(coordinate[1]) == 5):
            return ""
        area = vChars[vertical.index(coordinate[1]) - 1] + str(area)
        vertical.remove(coordinate[1])

    if (area in areas):
        return area
    else:
        return ""

def getScores(coordinates,message, areas, keywords):
    scores = {
        "A1": [0,0],"A2": [0,0],"A3": [0,0],"A4": [0,0],"B1": [0,0],"B2": [0,0],"B3": [0,0],"B4": [0,0],"C1": [0,0],"C2": [0,0],"C3": [0,0],"C4": [0,0],"C5": [0,0],"D3": [0,0],"D4": [0,0],"D5": [0,0]
    }

    area = getArea([float(coordinates.split(',')[0]), float(coordinates.split(',')[1])], areas)

    if area != "":
        t = message.lower()
        # count number of twitters
        scores[area][0] += 1
        # the built-in regexp complie() function runs slow, use hardcoding
        for j in keywords:
            # single word in a sentence
            if j in t and len(j) == len(t):
                scores[area][1] += keywords[j]
            elif j in t and t.startswith(j):
                if (j + " ") in t or (j + "!") in t or (j + ",") in t or (j + "?") in t or (j + ".") in t or (
                        j + '\\"') in t or (j + "\'") in t:
                    # multiple keyword check
                    count = t.count(j + " ") + t.count(j + "!") + t.count(j + ",") + t.count(j + "?") + t.count(
                        j + ".") + t.count(j + '\\"') + t.count(j + "\'")
                    scores[area][1] += count * keywords[j]
            elif (" " + j) in t:
                if t.endswith(j) or (j + " ") in t or (j + "!") in t or (j + ",") in t or (j + "?") in t or (
                        j + ".") in t or (j + '\\"') in t or (j + "\'") in t:
                    count = t.count(j + " ") + t.count(j + "!") + t.count(j + ",") + t.count(j + "?") + t.count(
                        j + ".") + t.count(j + '\\"') + t.count(j + "\'")
                    scores[area][1] += count * keywords[j]
    return scores


def main():
    mpi_comm = MPI.COMM_WORLD
    mpi_rank = mpi_comm.Get_rank()
    mpi_size = mpi_comm.Get_size()

    if mpi_rank == 0:
        print('Initiating the app...')
        print('The total number of tasks are: ' + str(mpi_size))

    keywords = putKeywords(open("AFINN.txt", 'r').read())
    gridFile = open("melbGrid.json", 'r').read()
    grid = json.loads(gridFile)
    areas = {}
    for i in range(len(grid["features"])):
        areas[grid["features"][i]["properties"]["id"]] = [grid["features"][i]["properties"]["xmin"],
                                                        grid["features"][i]["properties"]["xmax"],
                                                        grid["features"][i]["properties"]["ymin"],
                                                        grid["features"][i]["properties"]["ymax"]]

    if mpi_size == 1:
        return mpi_root(mpi_comm, keywords, areas, True)
    if mpi_rank == 0:
        return mpi_root(mpi_comm, keywords, areas, False)
    else:
        return mpi_nonroot(mpi_comm, keywords, areas)

def mpi_root(mpi_comm, keywords, areas, singleTask):
    mpi_rank = mpi_comm.Get_rank()
    mpi_size = mpi_comm.Get_size()

    scores = {
        "A1": [0,0],"A2": [0,0],"A3": [0,0],"A4": [0,0],"B1": [0,0],"B2": [0,0],"B3": [0,0],"B4": [0,0],"C1": [0,0],"C2": [0,0],"C3": [0,0],"C4": [0,0],"C5": [0,0],"D3": [0,0],"D4": [0,0],"D5": [0,0]
    }

    lineNumber = -1
    filesize = sys.argv[1]

    with open(filesize + ".json", 'r') as f:
        terminate = False
        while True:
            if terminate:
                break
            for line in f:
                if line.endswith("]}\n"):
                    terminate = True
                if not singleTask:
                    if lineNumber % mpi_size == mpi_rank:
                        if ('"coordinates":') in line and ('"properties":') in line and ('"text":') in line and (
                        '"location":') in line:
                            coordinates = line[line.index('"coordinates":') + 15: line.index('"properties":') - 3]
                            message = line[line.index('"text":') + 8: line.index('"location":') - 2]
                            rootScores = getScores(coordinates, message, areas, keywords)
                            for i in rootScores:
                                scores[i][0] += rootScores[i][0]
                                scores[i][1] += rootScores[i][1]
                else:
                    if ('"coordinates":') in line and ('"properties":') in line and ('"text":') in line and (
                            '"location":') in line:
                        coordinates = line[line.index('"coordinates":') + 15: line.index('"properties":') - 3]
                        message = line[line.index('"text":') + 8: line.index('"location":') - 2]
                        rootScores = getScores(coordinates, message, areas, keywords)
                        for i in rootScores:
                            scores[i][0] += rootScores[i][0]
                            scores[i][1] += rootScores[i][1]
                lineNumber += 1

        response_scores = mpi_comm.gather(None)
        for i in range(1, len(response_scores)):
            # one score dict per task
            for j in response_scores[i]:
                scores[j][0] += response_scores[i][j][0]
                scores[j][1] += response_scores[i][j][1]
        mpi_comm.barrier()


    print("Cell\t#Total Tweets\t#Overall Sentiment Score\n")
    for i in scores:
        print( i +"\t" + str(scores[i][0]) + "\t" + str(scores[i][1]) + "\n")
    return 0


def mpi_nonroot(mpi_comm, keywords, areas):
    mpi_rank = mpi_comm.Get_rank()
    mpi_size = mpi_comm.Get_size()

    scores = {
        "A1": [0,0],"A2": [0,0],"A3": [0,0],"A4": [0,0],"B1": [0,0],"B2": [0,0],"B3": [0,0],"B4": [0,0],"C1": [0,0],"C2": [0,0],"C3": [0,0],"C4": [0,0],"C5": [0,0],"D3": [0,0],"D4": [0,0],"D5": [0,0]
    }

    lineNumber = -1
    filesize = sys.argv[1]

    with open(filesize + ".json", 'r') as f:
        terminate = False
        while True:
            if terminate:
                break
            for line in f:
                if line.endswith("]}\n"):
                    terminate = True
                if lineNumber % mpi_size == mpi_rank:
                    if ('"coordinates":') in line and ('"properties":') in line and ('"text":') in line and (
                    '"location":') in line:
                        coordinates = line[line.index('"coordinates":') + 15: line.index('"properties":') - 3]
                        message = line[line.index('"text":') + 8: line.index('"location":') - 2]
                        rootScores = getScores(coordinates, message, areas, keywords)
                        for i in rootScores:
                            scores[i][0] += rootScores[i][0]
                            scores[i][1] += rootScores[i][1]
                lineNumber += 1

    mpi_comm.gather(scores)
    mpi_comm.barrier()
    return 0


if __name__ == '__main__':
    sys.exit(main())