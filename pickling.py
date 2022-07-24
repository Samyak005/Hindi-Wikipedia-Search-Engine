import timeit
import pickle
import sys



def pickleField(fileNo, field):
    fieldOffset = []
    with open(sys.argv[1] + '/offset_' + field + str(fileNo) + '.txt', 'r') as f:
        for line in f:
            fieldOffset.append(line)

    dbfile = open(sys.argv[1] + '/offset_' + field + str(fileNo) + '.pkl', 'wb')
    pickle.dump(fieldOffset, dbfile)                     
    dbfile.close()
    return 

if __name__ == '__main__':

    start = timeit.default_timer()   
    # offset = []
    # with open(sys.argv[1] + '/titleOffset.txt', 'r') as f:
    #     for line in f:
    #         offset.append(line)

    # dbfile = open(sys.argv[1] + '/titleOffset.pkl', 'wb')
    # pickle.dump(offset, dbfile)                     
    # dbfile.close()
    offset = []

    # with open(sys.argv[1] + '/offset.txt', 'r') as f:
    #     for line in f:
    #         offset.append(line)

    # dbfile = open(sys.argv[1] + '/offset.pkl', 'wb')
    # pickle.dump(offset, dbfile)                     
    # dbfile.close()
    # offset = []

    numBodyFiles, numTitleFiles, numInfoFiles, numRefFiles, numLinkFiles, numCategoryFiles = 0,0,0,0,0,0
    if sys.argv[1] == './dataSmallOutput/':
        numBodyFiles, numTitleFiles, numInfoFiles, numRefFiles, numLinkFiles, numCategoryFiles = 1, 1 , 1, 1 , 1 , 1
    if sys.argv[1] == './inverted_indexes/':
        numBodyFiles, numTitleFiles, numInfoFiles, numRefFiles, numLinkFiles, numCategoryFiles = 16, 3, 4, 1, 1, 1
    # numBodyFiles, numTitleFiles, numInfoFiles, numRefFiles, numLinkFiles, numCategoryFiles = 7, 1 , 2, 1 , 1 , 1

    for i in range(numBodyFiles):
        pickleField(i, 'b')

    for i in range(numTitleFiles):
        pickleField(i, 't')

    for i in range(numInfoFiles):
        pickleField(i, 'i')

    for i in range(numRefFiles):
        pickleField(i, 'r')

    for i in range(numLinkFiles):
        pickleField(i, 'l')

    for i in range(numCategoryFiles):
        pickleField(i, 'c')

    end = timeit.default_timer()
    print('Time taken =', end-start)

    # dbfile = open('examplePickle', 'rb')
      
    # # source, destination
    # offset1= pickle.load(dbfile)                     
    # dbfile.close()
    # end = timeit.default_timer()
    # print('Time taken =', end-start)  

