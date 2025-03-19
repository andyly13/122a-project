from terminal import * 


def main(): 
    
    UserName = 'test'
    PassWord = 'password'
    DataBase_Name = 'cs122a' 

    cur, con = connect(UserName, PassWord, DataBase_Name) 


    startingpoint = sys.argv[1]
    if startingpoint == 'import': 
        DB_creation(cur, DataBase_Name)
        #print("DB created")
        parsingfiles(cur, con, sys.argv[2])

    if startingpoint in ('insertViewer', 'insertMovie', 'insertSession'):
        insertions(cur, con, startingpoint, sys.argv[2:])

    if startingpoint in('deleteViewer'):
        deletions(cur, con, startingpoint, sys.argv)

    if startingpoint in ('addGenre'):
        uid = int(sys.argv[2])
        new_genre = sys.argv[3]
        addGenre(cur, con, uid, new_genre)

    if startingpoint in ('updateRelease'):
        updating(cur, con, sys.argv[1:])

    if startingpoint in ('listReleases'):
        uid = int(sys.argv[2])  
        releasereview(cur, con, uid) 

    if startingpoint == 'popularRelease':
        popular(cur, con, int(sys.argv[2]))

    # if startingpoint in ('releaseTitle'):
    #     return releaseTitle(cur, con, sys.argv)


if __name__ == "__main__":
    main()